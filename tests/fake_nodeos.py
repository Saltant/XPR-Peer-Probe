#!/usr/bin/env python3
"""Local test double only. NOT Antelope, NOT a usable nodeos implementation."""
import argparse
import json
import signal
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--full-version', action='store_true')
p.add_argument('--version', action='store_true')
p.add_argument('--snapshot')
p.add_argument('--config-dir')
p.add_argument('--data-dir')
a = p.parse_args()
if a.full_version or a.version:
    print('v5.0.3-TEST-DOUBLE')
    raise SystemExit(0)
config = {}
for line in (Path(a.config_dir) / 'config.ini').read_text().splitlines():
    if '=' in line:
        k,v = line.split('=',1)
        config[k.strip()] = v.strip()
fixture = json.loads(Path(a.snapshot).read_text())
port = int(config['http-server-address'].rsplit(':',1)[1])
start = int(fixture.get('head',100))
state = {'head':start,'socket':None,'endpoint':None,'handshake':None,'is_open':False}
lock = threading.Lock()
shutdown = threading.Event()


def disconnect():
    with lock:
        s = state['socket']
        state['socket'] = None
        state['is_open'] = False
    if s:
        try: s.shutdown(socket.SHUT_RDWR)
        except OSError: pass
        s.close()


def connect(endpoint):
    host,port = endpoint.rsplit(':',1)
    try:
        s = socket.create_connection((host.strip('[]'),int(port)),timeout=3)
        with lock:
            state.update(socket=s,is_open=True,endpoint=endpoint)
        s.sendall(json.dumps({'chain_id':fixture['chain_id'],'head':state['head']}).encode()+b'\n')
        print('Sending handshake generation 1',flush=True)
        f = s.makefile('rb')
        line = f.readline()
        if not line:
            print('Peer closed connection',flush=True)
            return
        hs=json.loads(line)
        if hs['chain_id'] != fixture['chain_id']:
            print('received go_away_message, reason = wrong chain',flush=True)
            return
        with lock: state['handshake']=hs
        s.sendall(json.dumps({'fetch':True,'from':state['head']+1}).encode()+b'\n')
        for line in f:
            msg=json.loads(line)
            if 'block' in msg:
                with lock: state['head']=max(state['head'],msg['block'])
    except Exception as exc:
        if not shutdown.is_set(): print('connection failed: '+str(exc),flush=True)
    finally:
        with lock: state['is_open']=False


class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args): pass
    def do_POST(self):
        raw=self.rfile.read(int(self.headers.get('Content-Length',0)))
        body=json.loads(raw or b'{}')
        code=200
        if self.path=='/v1/chain/get_info':
            response={'chain_id':fixture['chain_id'],'head_block_num':state['head'],
                      'last_irreversible_block_num':start,'head_block_id':f'{state["head"]:064x}',
                      'head_block_time':'2026-09-01T00:00:00.000'}
        elif self.path=='/v1/net/connections': response=[]
        elif self.path=='/v1/net/connect':
            if not isinstance(body,str):
                response={'error':'scalar string required'};code=400
            else:
                threading.Thread(target=connect,args=(body,),daemon=True).start()
                response='added connection'
        elif self.path=='/v1/net/status':
            with lock:
                response=None if state['endpoint'] is None else {
                    'connecting':False,'is_socket_open':state['is_open'],
                    'remote_ip':state['endpoint'].rsplit(':',1)[0],
                    'last_handshake':state['handshake'] or {}}
        elif self.path=='/v1/net/disconnect':
            disconnect();response='connection removed'
        else: response={'error':'unknown'};code=404
        data=json.dumps(response).encode()
        self.send_response(code)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(data)))
        self.end_headers()
        try:self.wfile.write(data)
        except OSError:pass


server=ThreadingHTTPServer(('127.0.0.1',port),Handler)
def terminate(*_):
    shutdown.set()
    disconnect()
    threading.Thread(target=server.shutdown,daemon=True).start()
signal.signal(signal.SIGTERM,terminate)
signal.signal(signal.SIGINT,terminate)
server.serve_forever(poll_interval=.02)
server.server_close()
# Mimic mapped_private allocating a state file during shutdown.
data_dir=Path(a.data_dir)
(data_dir/'state').mkdir(exist_ok=True)
(data_dir/'state'/'shared_memory.bin').write_bytes(b'\x00'*(512*1024))
