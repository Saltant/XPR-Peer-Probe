"""Hermetic tests: no public blockchain, no external network, no root installation."""
import contextlib
import importlib.util
import io
import json
import os
import signal
import socket
import socketserver
import struct
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('probe',ROOT/'xpr_peer_probe.py')
probe=importlib.util.module_from_spec(spec)
sys.modules['probe']=probe
spec.loader.exec_module(probe)


def exact(sock,n):
    result=b''
    while len(result)<n:
        more=sock.recv(n-len(result))
        if not more: raise EOFError
        result+=more
    return result


def free_ports(n=3):
    socks=[]
    try:
        for _ in range(n):
            s=socket.socket();s.bind(('127.0.0.1',0));socks.append(s)
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:s.close()


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address=True
    daemon_threads=True


class SocksHandler(socketserver.BaseRequestHandler):
    def handle(self):
        upstream=None
        try:
            version,nmethods=exact(self.request,2)
            methods=exact(self.request,nmethods)
            if self.server.reject_auth:
                self.request.sendall(b'\x05\xff');return
            method=2 if self.server.credentials else 0
            if version!=5 or method not in methods:
                self.request.sendall(b'\x05\xff');return
            # Fragment the SOCKS reply to verify receive_exact handling.
            self.request.sendall(b'\x05');self.request.sendall(bytes([method]))
            if method==2:
                v,n=exact(self.request,2);u=exact(self.request,n).decode()
                n=exact(self.request,1)[0];p=exact(self.request,n).decode()
                if (u,p)!=self.server.credentials:
                    self.request.sendall(b'\x01\x01');return
                self.request.sendall(b'\x01\x00')
            head=exact(self.request,4)
            if head[:3]!=b'\x05\x01\x00':return
            if head[3]==1:host=socket.inet_ntoa(exact(self.request,4))
            elif head[3]==4:host=socket.inet_ntop(socket.AF_INET6,exact(self.request,16))
            else:host=exact(self.request,exact(self.request,1)[0]).decode()
            port=struct.unpack('!H',exact(self.request,2))[0]
            self.server.targets.append((host,port))
            if self.server.reject_connect:
                self.request.sendall(b'\x05\x05\x00\x01'+b'\x00'*6);return
            upstream=socket.create_connection((host,port),timeout=3)
            upstream.settimeout(None)
            self.request.sendall(b'\x05\x00\x00\x01'+b'\x00'*6)
            def pipe(src,dst):
                try:
                    while True:
                        buf=src.recv(65536)
                        if not buf:break
                        dst.sendall(buf)
                except OSError:pass
                finally:
                    with contextlib.suppress(OSError):dst.shutdown(socket.SHUT_WR)
            t=threading.Thread(target=pipe,args=(self.request,upstream),daemon=True);t.start()
            pipe(upstream,self.request);t.join(2)
        except (EOFError,OSError):pass
        finally:
            if upstream:upstream.close()


class PeerHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.server.connections+=1
        behavior=self.server.behavior
        try:
            self.request.settimeout(5)
            f=self.request.makefile('rb')
            if behavior=='close':return
            line=f.readline()
            if behavior=='silent':time.sleep(2);return
            hello=json.loads(line)
            chain=hello['chain_id'] if behavior!='wrong' else 'f'*64
            hs={'chain_id':chain,'head_num':100000,'last_irreversible_block_num':99990,'agent':'mock'}
            if behavior=='short':hs.update(head_num=105,last_irreversible_block_num=104)
            self.request.sendall(json.dumps(hs).encode()+b'\n')
            line=f.readline()
            if behavior=='good':
                start=json.loads(line)['from']
                for i in range(start,start+100):
                    self.request.sendall(json.dumps({'block':i}).encode()+b'\n')
            while self.request.recv(1024):pass
        except (OSError,ValueError,TypeError):pass


class EchoHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # Return all bytes only after client half-close, exercising both half-closes.
        data=bytearray()
        while True:
            block=self.request.recv(65536)
            if not block:break
            data.extend(block)
        self.request.sendall(bytes(data))


@contextlib.contextmanager
def serving(handler,**kwargs):
    server=Server(('127.0.0.1',0),handler)
    server.reject_auth=False;server.reject_connect=False;server.credentials=None;server.targets=[]
    server.behavior='good';server.connections=0
    for k,v in kwargs.items():setattr(server,k,v)
    t=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01},daemon=True)
    t.start()
    try:yield server
    finally:server.shutdown();server.server_close();t.join(2)


class ParsingTests(unittest.TestCase):
    def test_endpoints_normalize(self):
        self.assertEqual(probe.normalize_endpoint('https://EXAMPLE.org:9876/'),'example.org:9876')
        self.assertEqual(probe.parse_endpoint('[::1]:123'),('::1',123))
        self.assertEqual(probe.normalize_endpoint('127.0.0.1:19000'),'127.0.0.1:19000')
    def test_invalid_endpoints(self):
        for value in ('foo','foo:0','foo:65536','http://foo:9876/path','a b:123','a:123?x','a:abc','::1:9876'):
            with self.subTest(value=value),self.assertRaises(probe.ProbeError):probe.parse_endpoint(value)
    def test_peers_filters_and_normalizes(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'peers.json'
            p.write_text(json.dumps([{'type':'p2p','status':'active','url':'http://a.test:9876'},
                 {'type':'api','status':'active','url':'b.test:9876'},
                 {'type':'p2p','status':'inactive','url':'c.test:9876'},'a.test:9876']))
            self.assertEqual(probe.load_peers(p),['a.test:9876'])
    def test_bad_json_not_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'x.json';p.write_text('[{invalid')
            with self.assertRaises(probe.ProbeError):probe.load_peers(p)
    def test_text_config_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'peers.txt';p.write_text('# comment\np2p-peer-address = a:99\nb:98 # note\n')
            self.assertEqual(probe.load_peers(p),['a:99','b:98'])
    def test_explicit_peer_bypasses_file(self):
        args=probe.make_parser().parse_args(['--network','testnet','--peer','a:9876','--peers-file','/no/such/file'])
        self.assertEqual(probe.select_peers(args)[0],['a:9876'])
    def test_network_defaults_separate(self):
        p=probe.make_parser()
        a=p.parse_args(['--network','mainnet']);b=p.parse_args(['--network','testnet'])
        probe.validate_args(a);probe.validate_args(b)
        self.assertEqual((a.http_port,b.http_port),(18888,28888))
    def test_explicit_exclusion_no_saltant_default(self):
        a=probe.make_parser().parse_args(['--network','mainnet','--peers','a:1,b:2','--exclude-peer','a:1'])
        self.assertEqual(probe.select_peers(a)[0],['b:2'])
    def test_recommendation_requires_every_round(self):
        rows=[dict(endpoint='a:1',phase='B',network_mode='DIRECT',classification='GOOD',blocks_per_second=20)]
        self.assertEqual(probe.recommendation_rows(rows,'DIRECT',2,10),[])
        self.assertEqual(len(probe.recommendation_rows(rows,'DIRECT',1,10)),1)
    def test_stale_closed_handshake_not_valid(self):
        self.assertIsNone(probe.valid_handshake({'is_socket_open':False,'last_handshake':{'chain_id':'x','head_num':10}}))
    def test_conflicting_options(self):
        for opts in (['--snapshot','a','--refresh-snapshot'],['--compare'],
                     ['--compare','--native-socks5','localhost:1','--relay-remote-dns']):
            a=probe.make_parser().parse_args(['--network','mainnet']+opts)
            with self.assertRaises(probe.ProbeError):probe.validate_args(a)


class SafetyTests(unittest.TestCase):
    def make_tar(self,path,entries):
        with tarfile.open(path,'w:gz') as tf:
            for name,type_,body in entries:
                m=tarfile.TarInfo(name);m.type=type_;m.size=len(body) if type_==tarfile.REGTYPE else 0
                m.linkname='/tmp/evil'
                tf.addfile(m,io.BytesIO(body) if m.size else None)
    def test_extract_single_bin(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp);self.make_tar(p/'a.tar.gz',[('./snap.bin',tarfile.REGTYPE,b'abc')])
            probe.extract_snapshot(p/'a.tar.gz',p/'out',100)
            self.assertEqual((p/'out').read_bytes(),b'abc')
    def test_reject_bad_archives(self):
        cases=[('../bad.bin',tarfile.REGTYPE,b'abc'),('/bad.bin',tarfile.REGTYPE,b'abc'),
               ('sym',tarfile.SYMTYPE,b''),('hard',tarfile.LNKTYPE,b''),('dev',tarfile.CHRTYPE,b'')]
        for entry in cases:
            with self.subTest(entry=entry),tempfile.TemporaryDirectory() as tmp:
                p=Path(tmp);self.make_tar(p/'a.tar.gz',[entry])
                with self.assertRaises(probe.ProbeError):probe.extract_snapshot(p/'a.tar.gz',p/'out',100)
    def test_reject_multiple_or_large_bin(self):
        for entries,limit in [([('a.bin',tarfile.REGTYPE,b'a'),('b.bin',tarfile.REGTYPE,b'b')],100),
                              ([('a.bin',tarfile.REGTYPE,b'abcdef')],2)]:
            with tempfile.TemporaryDirectory() as tmp:
                p=Path(tmp);self.make_tar(p/'a.tar.gz',entries)
                with self.assertRaises(probe.ProbeError):probe.extract_snapshot(p/'a.tar.gz',p/'out',limit)
    def test_lock_excludes_second_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            with probe.WorkspaceLock(Path(tmp)):
                with self.assertRaises(probe.ProbeError):
                    with probe.WorkspaceLock(Path(tmp)):pass
    def test_safe_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp);(p/'run.json').write_text('{}');(p/'data'/'one').mkdir(parents=True)
            outside=p/'important';outside.mkdir()
            with self.assertRaises(probe.ProbeError):probe.remove_owned_data(outside,p)
            probe.remove_owned_data(p/'data'/'one',p)
            self.assertTrue(outside.exists())
    def test_cleanup_refuses_symlink(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp);(p/'run.json').write_text('{}');(p/'data').mkdir();(p/'outside').mkdir()
            (p/'data'/'link').symlink_to(p/'outside',target_is_directory=True)
            with self.assertRaises(probe.ProbeError):probe.remove_owned_data(p/'data'/'link',p)
    def test_bound_port_is_not_free(self):
        with socket.socket() as s:
            s.bind(('127.0.0.1',0));s.listen()
            with self.assertRaises(probe.ProbeError):probe.check_ports([s.getsockname()[1]])
    def test_listener_ownership_is_checked(self):
        with socket.socket() as s:
            s.bind(('127.0.0.1',0));s.listen()
            port=s.getsockname()[1]
            self.assertTrue(probe.owns_listening_port(os.getpid(),port))
            self.assertFalse(probe.owns_listening_port(99999999,port))
    def test_wrong_snapshot_is_local_error(self):
        info={'chain_id':'f'*64,'head_block_num':100,'last_irreversible_block_num':100}
        with self.assertRaisesRegex(probe.ProbeError,'LOCAL SNAPSHOT'):probe.validate_snapshot_state(info,'mainnet',None)
    def test_cache_hash_mismatch_before_test(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);snap=root/'s.bin';snap.write_bytes(b'data')
            a=probe.make_parser().parse_args(['--network','mainnet','--snapshot',str(snap),'--snapshot-sha256','0'*64])
            with self.assertRaisesRegex(probe.ProbeError,'SHA-256'):probe.prepare_snapshot(a,root)
    def test_local_rpc_ignores_http_proxy_environment(self):
        # HTTPConnection is used directly; there is no urllib proxy handler in Nodeos.api.
        self.assertIn('HTTPConnection',probe.Nodeos.api.__code__.co_names)
    def test_log_wrong_chain_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'a.log';p.write_text('received go_away_message, reason = wrong chain\n')
            tail=probe.LogTail(p)
            self.assertEqual(tail.read(),'WRONG_CHAIN')
            self.assertTrue(tail.evidence)


class DependencyTests(unittest.TestCase):
    def test_runtime_plan_refuses_upgrades(self):
        dep=subprocess.CompletedProcess([],0,'libdemo (>= 1.0)')
        plan=subprocess.CompletedProcess([],0,'Inst libdemo [1.0] (2.0 Ubuntu:stable)\n')
        with mock.patch.object(probe,'run_command',side_effect=[dep,plan]), \
             mock.patch.object(probe.shutil,'which',return_value='/usr/bin/apt-get'), \
             mock.patch.object(probe.subprocess,'run') as actual:
            with self.assertRaises(probe.ProbeError):probe.install_runtime_dependencies(Path('signed.deb'),True)
            actual.assert_not_called()
    def test_runtime_plan_requires_approval(self):
        dep=subprocess.CompletedProcess([],0,'libdemo')
        plan=subprocess.CompletedProcess([],0,'Inst libdemo (1.0 Ubuntu:stable)\n')
        with mock.patch.object(probe,'run_command',side_effect=[dep,plan]), \
             mock.patch.object(probe.shutil,'which',return_value='/usr/bin/apt-get'), \
             mock.patch.object(probe,'ask_permission',return_value=False), \
             mock.patch.object(probe.subprocess,'run') as actual:
            with self.assertRaises(probe.ProbeError):probe.install_runtime_dependencies(Path('signed.deb'),False)
            actual.assert_not_called()
    def test_existing_nodeos_is_not_reinstalled(self):
        args=probe.make_parser().parse_args(['--network','mainnet','--nodeos-bin',str(ROOT/'tests'/'fake_nodeos.py'),'--install-deps','--yes'])
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch.object(probe,'install_local_nodeos') as install:
            path,version=probe.ensure_nodeos(Path(tmp),args)
            self.assertIn('5.0.3',version)
            install.assert_not_called()
    def test_automatic_install_rejects_unsupported_os(self):
        args=probe.make_parser().parse_args(['--network','mainnet','--install-deps','--yes'])
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch.object(probe.platform,'machine',return_value='aarch64'):
            with self.assertRaisesRegex(probe.ProbeError,'amd64'):probe.install_local_nodeos(Path(tmp),args)


class RelayTests(unittest.TestCase):
    def test_socks_auth_failure(self):
        with serving(SocksHandler,reject_auth=True) as s:
            with self.assertRaises(probe.ProxyError):probe.open_socks(s.server_address,None,2)
    def test_socks_user_password(self):
        with serving(SocksHandler,credentials=('user','password')) as s:
            with probe.open_socks(s.server_address,None,2,('user','password')):pass
            with self.assertRaises(probe.ProxyError):probe.open_socks(s.server_address,None,2,('user','bad'))
    def test_native_binary_bidirectional_and_half_close(self):
        data=os.urandom(1024*1024+173)
        with serving(EchoHandler) as echo,serving(SocksHandler) as socks:
            relay=probe.NativeRelay(0,socks.server_address,echo.server_address,2)
            endpoint=relay.start()
            try:
                with socket.create_connection(probe.parse_endpoint(endpoint),timeout=5) as c:
                    c.sendall(data);c.shutdown(socket.SHUT_WR)
                    received=b''
                    while True:
                        more=c.recv(65536)
                        if not more:break
                        received+=more
                self.assertEqual(received,data)
            finally:relay.stop()
            self.assertEqual(relay.stats['nodeos_to_peer_bytes'],len(data))
            self.assertEqual(relay.stats['peer_to_nodeos_bytes'],len(data))
            self.assertEqual(relay.errors,[])
    def test_native_reports_socks_connect_rejection(self):
        with serving(SocksHandler,reject_connect=True) as socks:
            relay=probe.NativeRelay(0,socks.server_address,('127.0.0.1',99),2)
            ep=relay.start()
            try:
                with socket.create_connection(probe.parse_endpoint(ep)) as c:
                    c.sendall(b'hello');c.settimeout(3)
                    with contextlib.suppress(OSError):c.recv(1)
                time.sleep(.1)
            finally:relay.stop()
            self.assertTrue(any('REP=5' in e for e in relay.errors))


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.snap=self.root/'snapshot.bin'
        self.snap.write_text(json.dumps({'chain_id':probe.NETWORKS['mainnet']['chain_id'],'head':100}))
    def tearDown(self):self.tmp.cleanup()
    def command(self,endpoint,extra=None):
        ports=free_ports()
        return [sys.executable,str(ROOT/'xpr_peer_probe.py'),'--network','mainnet',
                '--workspace',str(self.root/'work'),'--nodeos-bin',str(ROOT/'tests'/'fake_nodeos.py'),
                '--snapshot',str(self.snap),'--peer',endpoint,'--http-port',str(ports[0]),
                '--p2p-port',str(ports[1]),'--relay-port',str(ports[2]),
                '--catchup-blocks','25','--handshake-timeout','1','--catchup-timeout','3',
                '--stall-seconds','0.3','--api-start-timeout','5','--shutdown-timeout','3',
                '--poll-interval','0.02','--pause','0.01','--min-free-gib','0.001','--min-ram-gib','0.001']+(extra or [])
    def run_cli(self,endpoint,extra=None):
        cp=subprocess.run(self.command(endpoint,extra),text=True,capture_output=True,timeout=20)
        latest=self.root/'work'/'mainnet'/'latest.json'
        report=None
        if latest.exists():
            results=Path(json.loads(latest.read_text())['results_dir'])
            report=json.loads((results/'results.json').read_text())
        return cp,report
    def test_direct_success_preserved_runs_and_shutdown_size(self):
        with serving(PeerHandler) as peer:
            ep=probe.endpoint_text(*peer.server_address)
            cp,report=self.run_cli(ep)
            self.assertEqual(cp.returncode,0,cp.stdout+cp.stderr)
            self.assertEqual(report['samples'][0]['classification'],'GOOD')
            self.assertGreater(report['samples'][0]['data_disk_bytes_after_stop'],500000)
            first=report['run_id']
            cp,second=self.run_cli(ep)
            self.assertEqual(cp.returncode,0,cp.stdout+cp.stderr)
            self.assertNotEqual(first,second['run_id'])
            self.assertTrue((self.root/'work'/'mainnet'/'runs'/first/'results'/'results.json').exists())
            self.assertFalse(list((self.root/'work').rglob('nodeos.pid.json')))
            self.assertFalse(list((self.root/'work').rglob('shared_memory.bin')))
    def test_compare_native_same_ip_and_snapshot(self):
        with serving(PeerHandler) as peer,serving(SocksHandler) as socks:
            cp,r=self.run_cli(probe.endpoint_text(*peer.server_address),
                     ['--compare','--native-socks5',probe.endpoint_text(*socks.server_address)])
            self.assertEqual(cp.returncode,0,cp.stdout+cp.stderr)
            self.assertEqual([x['classification'] for x in r['samples']],['GOOD','GOOD'])
            self.assertEqual(r['samples'][0]['selected_ip'],r['samples'][1]['selected_ip'])
            self.assertGreater(r['samples'][1]['relay']['peer_to_nodeos_bytes'],0)
    def test_wrong_chain_peer(self):
        with serving(PeerHandler,behavior='wrong') as peer:
            cp,r=self.run_cli(probe.endpoint_text(*peer.server_address))
            self.assertEqual(cp.returncode,3,cp.stdout+cp.stderr)
            self.assertEqual(r['samples'][0]['classification'],'WRONG_CHAIN')
    def test_slow_peer(self):
        with serving(PeerHandler,behavior='slow') as peer:
            cp,r=self.run_cli(probe.endpoint_text(*peer.server_address))
            self.assertEqual(cp.returncode,3,cp.stdout+cp.stderr)
            self.assertEqual(r['samples'][0]['classification'],'SLOW')
            self.assertEqual(r['samples'][0]['gained_blocks'],0)
    def test_silent_peer(self):
        with serving(PeerHandler,behavior='silent') as peer:
            cp,r=self.run_cli(probe.endpoint_text(*peer.server_address))
            self.assertEqual(cp.returncode,3,cp.stdout+cp.stderr)
            self.assertEqual(r['samples'][0]['classification'],'SILENT')
    def test_short_history_is_not_good(self):
        with serving(PeerHandler,behavior='short') as peer:
            cp,r=self.run_cli(probe.endpoint_text(*peer.server_address))
            self.assertEqual(cp.returncode,3,cp.stdout+cp.stderr)
            self.assertEqual(r['samples'][0]['classification'],'INSUFFICIENT_HISTORY')
    def test_bad_snapshot_never_connects_to_peer(self):
        self.snap.write_text(json.dumps({'chain_id':probe.NETWORKS['testnet']['chain_id'],'head':100}))
        with serving(PeerHandler) as peer:
            cp,r=self.run_cli(probe.endpoint_text(*peer.server_address))
            self.assertEqual(cp.returncode,2,cp.stdout+cp.stderr)
            self.assertEqual(peer.connections,0)
            self.assertEqual(r['state'],'FAILED')
            self.assertFalse(list((self.root/'work').rglob('nodeos.pid.json')))
    def test_phase_a_supported_through_native_relay(self):
        with serving(PeerHandler) as peer,serving(SocksHandler) as socks:
            cp,r=self.run_cli(probe.endpoint_text(*peer.server_address),
                ['--native-socks5',probe.endpoint_text(*socks.server_address),'--with-phase-a','--phase-a-hold','0.05'])
            self.assertEqual(cp.returncode,0,cp.stdout+cp.stderr)
            self.assertEqual([x['classification'] for x in r['samples']],['HANDSHAKE_OK','GOOD'])
    def test_ctrl_c_stops_owned_node_and_preserves_report(self):
        with serving(PeerHandler,behavior='slow') as peer:
            command=self.command(probe.endpoint_text(*peer.server_address),['--stall-seconds','20','--catchup-timeout','30'])
            proc=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
            deadline=time.monotonic()+8
            while time.monotonic()<deadline and not list((self.root/'work').glob('mainnet/runs/*/nodeos.pid.json')):
                if proc.poll() is not None:break
                time.sleep(.05)
            proc.send_signal(signal.SIGINT)
            output=proc.communicate(timeout=8)[0]
            self.assertEqual(proc.returncode,130,output)
            self.assertFalse(list((self.root/'work').rglob('nodeos.pid.json')))
            latest=json.loads((self.root/'work'/'mainnet'/'latest.json').read_text())
            self.assertEqual(latest['state'],'INTERRUPTED')


if __name__=='__main__':unittest.main()
