#!/usr/bin/env python3
"""XPR Peer Probe: isolated nodeos P2P handshake and historical catch-up tests.

Run ./setup.sh on a new Ubuntu host, or python3 xpr_peer_probe.py --help.
Only Python's standard library is used. See README.md and docs/CLI.md.
"""
from __future__ import annotations

import sys
if sys.version_info < (3, 10):
    sys.exit("Python 3.10+ is required. Run ./setup.sh or install a supported Python.")

import argparse
import contextlib
import csv
import datetime as dt
import fcntl
import hashlib
import http.client
import ipaddress
import json
import math
import os
import platform
import queue
import re
import resource
import selectors
import shlex
import shutil
import signal
import socket
import ssl
import stat
import struct
import subprocess
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable

VERSION = "2.0.0-rc2"
GIB = 1024 ** 3
SCRIPT_DIR = Path(__file__).resolve().parent
LEAP_VERSION = "5.0.3"  # Explicitly pinned; never install GitHub's arbitrary 'latest'.
LEAP_URL = ("https://github.com/AntelopeIO/leap/releases/download/v5.0.3/"
            "leap_5.0.3_amd64.deb")
MAINTAINER_KEYS = ["https://github.com/arhag.gpg", "https://github.com/ericpassmore.gpg",
                   "https://github.com/spoonincode.gpg"]
NETWORKS = {
    "mainnet": {
        "chain_id": "384da888112027f0321850a169f737c33e53b388aad48b5adace4bab97f437e0",
        "snapshot_url": "https://snapshots.eosusa.io/snapshots/proton/latest.tar.gz",
        "http_port": 18888, "p2p_port": 19876, "relay_port": 19000,
    },
    "testnet": {
        "chain_id": "71ee83bcf52142d61019d95f9cc5427ba6a0d7ff8accd9e2088ae2abeaf3d3dd",
        "snapshot_url": "https://snapshots.eosusa.io/snapshots/protontestnet/latest.tar.gz",
        "http_port": 28888, "p2p_port": 29876, "relay_port": 29000,
    },
}


class ProbeError(RuntimeError):
    """Local setup/runtime failure, not a remote peer verdict."""


class ProxyError(ProbeError):
    pass


def utcnow() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def say(message: str) -> None:
    print(message, flush=True)


def human_bytes(value: int | float) -> str:
    number = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(number) < 1024 or unit == "TiB":
            return f"{number:.2f} {unit}"
        number /= 1024
    return str(value)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_semver(text: str) -> str | None:
    """Return the first X.Y.Z semantic version from nodeos version output."""
    match = re.search(r'(?<!\d)(\d+)\.(\d+)\.(\d+)(?!\d)', text)
    return '.'.join(match.groups()) if match else None


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with tmp.open("x", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def parse_endpoint(value: str) -> tuple[str, int]:
    """Accept host:port, [IPv6]:port; trim an accidental HTTP prefix explicitly."""
    value = value.strip()
    if value.startswith(("http://", "https://")):
        parsed = urllib.parse.urlsplit(value)
        if parsed.path not in ("", "/") or parsed.query or parsed.fragment or parsed.username:
            raise ProbeError(f"P2P endpoint must not contain a path or credentials: {value!r}")
        value = parsed.netloc
    if not value or any(c.isspace() for c in value):
        raise ProbeError(f"Invalid endpoint: {value!r}")
    if value.startswith("["):
        match = re.fullmatch(r"\[([^\]]+)\]:(\d+)", value)
        if not match:
            raise ProbeError(f"Use [IPv6]:port: {value!r}")
        host, text_port = match.groups()
        try:
            ipaddress.IPv6Address(host)
        except ValueError as exc:
            raise ProbeError(f"Invalid IPv6 address: {host}") from exc
    else:
        if value.count(":") != 1:
            raise ProbeError(f"Use host:port or [IPv6]:port: {value!r}")
        host, text_port = value.rsplit(":", 1)
        try:
            host = host.encode("idna").decode("ascii").lower().rstrip(".")
        except UnicodeError as exc:
            raise ProbeError(f"Invalid hostname: {host!r}") from exc
        if not re.fullmatch(r"[a-zA-Z0-9_.-]+", host) or not host or host.startswith("-"):
            raise ProbeError(f"Invalid hostname: {host!r}")
    if not text_port.isdecimal() or not 1 <= int(text_port) <= 65535:
        raise ProbeError(f"Invalid port: {value!r}")
    return host, int(text_port)


def endpoint_text(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def normalize_endpoint(value: str, notify: Callable[[str], None] | None = None) -> str:
    normalized = endpoint_text(*parse_endpoint(value))
    if value.strip() != normalized and notify:
        notify(f"Normalized endpoint: {value!r} -> {normalized}")
    return normalized


def load_peers(path: Path) -> list[str]:
    try:
        text = path.read_text(encoding="utf-8-sig")
    except OSError as exc:
        raise ProbeError(f"Cannot read peers file {path}: {exc}") from exc
    if text.lstrip().startswith(("[", "{")) or path.suffix.lower() == ".json":
        try:
            raw = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ProbeError(f"Invalid JSON in {path}: {exc}") from exc
        if not isinstance(raw, list):
            raise ProbeError("Peers JSON must be an array of strings or endpoint objects.")
        values = []
        for i, item in enumerate(raw, 1):
            if isinstance(item, str):
                values.append(item)
            elif isinstance(item, dict):
                if item.get("type", "p2p") != "p2p" or item.get("status", "active") != "active":
                    continue
                if not isinstance(item.get("url"), str):
                    raise ProbeError(f"Entry #{i} in {path} has no string 'url'.")
                values.append(item["url"])
            else:
                raise ProbeError(f"Invalid entry #{i} in {path}")
    else:
        values = []
        for line in text.splitlines():
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            if line.startswith("p2p-peer-address"):
                if "=" not in line:
                    raise ProbeError(f"Invalid config line: {line}")
                line = line.split("=", 1)[1].strip()
            values.append(line)
    return list(dict.fromkeys(normalize_endpoint(x, say) for x in values))


def select_peers(args: argparse.Namespace) -> tuple[list[str], str]:
    explicit = list(args.peer)
    for group in args.peers:
        explicit.extend(x for x in group.split(",") if x.strip())
    if explicit:
        peers = list(dict.fromkeys(normalize_endpoint(x, say) for x in explicit))
        source = "command line"
    else:
        path = Path(args.peers_file).expanduser() if args.peers_file else (
            SCRIPT_DIR / "peers" / f"{args.network}.json")
        peers = load_peers(path)
        source = str(path.resolve())
    excluded = {normalize_endpoint(x) for x in args.exclude_peer}
    for peer in peers:
        if peer in excluded:
            say(f"Excluded by request: {peer}")
    peers = [x for x in peers if x not in excluded]
    if not peers:
        raise ProbeError("No peers selected. Check the file, filters and exclusions.")
    return peers, source


def resolve_host(host: str, port: int, family: str, timeout: float) -> list[str]:
    """Bound caller wait even when libc DNS resolver takes a long time."""
    result: queue.Queue = queue.Queue(maxsize=1)
    af = socket.AF_INET if family == "4" else socket.AF_INET6
    def worker() -> None:
        try:
            rows = socket.getaddrinfo(host, port, family=af, type=socket.SOCK_STREAM)
            result.put((list(dict.fromkeys(row[4][0] for row in rows)), None))
        except Exception as exc:
            result.put(([], str(exc)))
    threading.Thread(target=worker, daemon=True, name="probe-dns").start()
    try:
        ips, error = result.get(timeout=timeout)
    except queue.Empty as exc:
        raise ProbeError(f"DNS timeout for {host}") from exc
    if error or not ips:
        raise ProbeError(f"DNS lookup failed for {host}: {error or 'no addresses'}")
    return ips


def tree_size(path: Path) -> tuple[int, int]:
    allocated = apparent = 0
    seen: set[tuple[int, int]] = set()
    if not path.exists():
        return 0, 0
    # Do not follow symlinks or count a hardlinked file twice.
    entries = [path]
    if path.is_dir() and not path.is_symlink():
        for root, dirs, files in os.walk(path, followlinks=False):
            entries.extend(Path(root) / n for n in dirs + files)
    for entry in entries:
        try:
            st = entry.lstat()
        except FileNotFoundError:
            continue
        key = (st.st_dev, st.st_ino)
        if key in seen:
            continue
        seen.add(key)
        allocated += st.st_blocks * 512
        if not stat.S_ISDIR(st.st_mode):
            apparent += st.st_size
    return allocated, apparent


def remove_owned_data(path: Path, run_dir: Path) -> None:
    """Delete only a data directory created inside this exact run."""
    if path.is_symlink() or path.resolve().parent != (run_dir / "data").resolve():
        raise ProbeError(f"Refusing unsafe cleanup: {path}")
    if not (run_dir / "run.json").is_file():
        raise ProbeError(f"Missing run ownership marker: {run_dir}")
    if path.exists():
        shutil.rmtree(path)


class WorkspaceLock:
    def __init__(self, root: Path):
        self.root = root
        self.handle = None

    def __enter__(self):
        self.root.mkdir(parents=True, exist_ok=True)
        if self.root.is_symlink():
            raise ProbeError("Workspace must not be a symlink.")
        path = self.root / ".probe.lock"
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        self.handle = os.fdopen(fd, "r+")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.handle.close()
            raise ProbeError(f"Another probe is using {self.root}. No files were cleared.") from exc
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(f"pid={os.getpid()}\n")
        self.handle.flush()
        return self

    def __exit__(self, *_):
        if self.handle:
            self.handle.close()


def check_ports(ports: list[int]) -> None:
    if len(set(ports)) != len(ports):
        raise ProbeError("HTTP, P2P and relay ports must be different.")
    sockets = []
    try:
        for port in ports:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sockets.append(s)
            try:
                s.bind(("127.0.0.1", port))
            except OSError as exc:
                raise ProbeError(f"127.0.0.1:{port} is unavailable: {exc}. Choose another port.") from exc
    finally:
        for s in sockets:
            s.close()


def check_orphan(root: Path) -> None:
    for marker in root.glob("*/runs/*/nodeos.pid.json"):
        try:
            info = json.loads(marker.read_text())
            pid = int(info["pid"])
            proc = Path(f"/proc/{pid}/cmdline")
            if proc.exists() and info.get("data_dir", "").encode() in proc.read_bytes():
                raise ProbeError(f"A previous diagnostic nodeos is still running (PID {pid}). "
                                 f"Inspect {marker}; stop it gracefully before testing. Nothing deleted.")
        except (ValueError, KeyError, OSError, json.JSONDecodeError):
            continue


def memory_available() -> int | None:
    try:
        raw = Path("/proc/meminfo").read_text()
        value = int(re.search(r"^MemAvailable:\s+(\d+)", raw, re.M)[1]) * 1024
        # On cgroup v2 hosts, also respect the current cgroup's memory budget.
        cgline = next((x for x in Path('/proc/self/cgroup').read_text().splitlines()
                       if x.startswith('0::')), None)
        if cgline:
            cg = Path('/sys/fs/cgroup') / cgline[3:].lstrip('/')
            maximum = (cg / 'memory.max').read_text().strip()
            if maximum.isdecimal():
                value = min(value, max(0, int(maximum) - int((cg / 'memory.current').read_text())))
        return value
    except (OSError, ValueError, TypeError):
        try:
            return int(re.search(r"^MemAvailable:\s+(\d+)", raw, re.M)[1]) * 1024
        except Exception:
            return None


def check_resources(root: Path, args: argparse.Namespace) -> dict:
    free = shutil.disk_usage(root).free
    ram = memory_available()
    say(f"Free disk: {human_bytes(free)}; available RAM: {human_bytes(ram) if ram is not None else 'unknown'}")
    problems = []
    if free < args.min_free_gib * GIB:
        problems.append(f"need at least {args.min_free_gib:g} GiB free disk")
    if ram is not None and ram < args.min_ram_gib * GIB:
        problems.append(f"need at least {args.min_ram_gib:g} GiB available RAM")
    if problems:
        message = "; ".join(problems)
        if not args.allow_low_resources:
            raise ProbeError(message + ". Free resources or explicitly use --allow-low-resources.")
        say("WARNING: resource guard overridden: " + message)
    return {"free_disk_bytes": free, "available_ram_bytes": ram}


def ask_permission(message: str, yes: bool = False) -> bool:
    say(message)
    if yes:
        return True
    if not sys.stdin.isatty():
        return False
    return input("Continue? [y/N] ").strip().lower() in ("y", "yes")


class HTTPSOnlyRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if urllib.parse.urlsplit(newurl).scheme != "https":
            raise ProbeError(f"Refusing HTTPS downgrade: {newurl}")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def download(url: str, destination: Path, limit: int, reserve: int = 0) -> None:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "https" or parsed.username or parsed.password:
        raise ProbeError("Downloads require an HTTPS URL without embedded credentials.")
    # All downloads are explicit HTTPS. The native relay only proxies P2P, not downloads.
    opener = urllib.request.build_opener(HTTPSOnlyRedirect())
    request = urllib.request.Request(url, headers={"User-Agent": f"XPR-Peer-Probe/{VERSION}"})
    say(f"Downloading {url}")
    started = last = time.monotonic()
    done = 0
    try:
        with opener.open(request, timeout=60) as response, destination.open("xb") as out:
            total = int(response.headers.get("Content-Length", 0) or 0)
            if total > limit:
                raise ProbeError(f"Download exceeds size limit ({human_bytes(limit)}).")
            if total and shutil.disk_usage(destination.parent).free < total + reserve:
                raise ProbeError("Insufficient space for download plus free-space reserve.")
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                done += len(chunk)
                if done > limit:
                    raise ProbeError("Download exceeded the configured size limit.")
                out.write(chunk)
                now = time.monotonic()
                if now - last > 2:
                    if shutil.disk_usage(destination.parent).free < reserve:
                        raise ProbeError("Free disk reserve exhausted during download.")
                    say(f"  {human_bytes(done)}" + (f" / {human_bytes(total)}" if total else ""))
                    last = now
            if total and done != total:
                raise ProbeError(f"Incomplete download: {done}/{total} bytes.")
            out.flush()
            os.fsync(out.fileno())
        say(f"Downloaded {human_bytes(done)} in {time.monotonic() - started:.1f}s")
    except BaseException:
        destination.unlink(missing_ok=True)
        raise


def extract_snapshot(archive: Path, output: Path, limit: int, reserve: int = 0) -> None:
    """Read only the single .bin as data; NEVER extract arbitrary archive paths."""
    with tarfile.open(archive, "r:*") as tf:
        snapshot = None
        count = 0
        for member in tf:
            count += 1
            if count > 10000:
                raise ProbeError("Archive has too many entries.")
            p = PurePosixPath(member.name)
            if p.is_absolute() or ".." in p.parts or "\\" in member.name:
                raise ProbeError(f"Unsafe archive path: {member.name!r}")
            if not (member.isdir() or member.isreg()):
                raise ProbeError(f"Archive links/devices are not accepted: {member.name!r}")
            if member.isreg() and member.name.lower().endswith(".bin"):
                if snapshot is not None:
                    raise ProbeError("Archive has multiple .bin files; use --snapshot with a chosen file.")
                snapshot = member
        if snapshot is None:
            raise ProbeError("Archive contains no .bin snapshot.")
        if snapshot.size <= 0 or snapshot.size > limit:
            raise ProbeError("Snapshot declared size is empty or exceeds --max-snapshot-gib.")
        if shutil.disk_usage(output.parent).free < snapshot.size + reserve:
            raise ProbeError("Not enough free disk to unpack snapshot with reserve.")
        source = tf.extractfile(snapshot)
        if source is None:
            raise ProbeError("Cannot read snapshot from archive.")
        with source, output.open("xb") as dest:
            size = 0
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                size += len(chunk)
                if size > limit:
                    raise ProbeError("Unpacked snapshot exceeds limit.")
                dest.write(chunk)
            if size != snapshot.size:
                raise ProbeError("Truncated snapshot in archive.")


def prepare_snapshot(args: argparse.Namespace, root: Path) -> tuple[Path, str, dict]:
    cache = root / args.network / "snapshots"
    cache.mkdir(parents=True, exist_ok=True)
    manifest_path = cache / "current.json"
    meta: dict = {}
    if args.snapshot:
        path = Path(args.snapshot).expanduser().resolve(strict=True)
        if not path.is_file() or path.stat().st_size == 0:
            raise ProbeError("--snapshot must refer to a nonempty regular file.")
        meta = {"source": "explicit_file", "filename": path.name}
    elif not args.refresh_snapshot and not args.snapshot_url and manifest_path.exists():
        try:
            meta = json.loads(manifest_path.read_text())
            name = meta["filename"]
            if not re.fullmatch(r"[0-9a-f]{64}\.bin", name):
                raise ProbeError("Invalid snapshot cache manifest.")
            path = cache / name
            if path.is_symlink() or not path.is_file():
                raise ProbeError("Cached snapshot is missing/unsafe. Use --refresh-snapshot.")
        except (KeyError, ValueError) as exc:
            raise ProbeError("Invalid snapshot cache metadata; use --refresh-snapshot.") from exc
    else:
        if args.skip_download:
            raise ProbeError("No selected cached snapshot. Use --snapshot PATH or allow download.")
        url = args.snapshot_url or NETWORKS[args.network]["snapshot_url"]
        with tempfile.TemporaryDirectory(prefix=".fetch-", dir=cache) as staging_name:
            staging = Path(staging_name)
            archive = staging / "download"
            reserve = 0 if args.allow_low_resources else int(args.min_free_gib * GIB)
            download(url, archive, int(args.max_download_gib * GIB), reserve)
            raw_bin = staging / "snapshot.bin"
            fmt = args.snapshot_format
            if fmt == "auto":
                fmt = "tar" if tarfile.is_tarfile(archive) else "bin"
            if fmt == "tar":
                extract_snapshot(archive, raw_bin, int(args.max_snapshot_gib * GIB), reserve)
            else:
                if archive.stat().st_size > args.max_snapshot_gib * GIB:
                    raise ProbeError("Raw snapshot exceeds --max-snapshot-gib.")
                archive.rename(raw_bin)
            digest = sha256_file(raw_bin)
            if args.snapshot_sha256 and digest != args.snapshot_sha256.lower():
                raise ProbeError("Downloaded snapshot SHA-256 mismatch; previous cache unchanged.")
            path = cache / f"{digest}.bin"
            if path.is_symlink():
                raise ProbeError("Refusing to replace snapshot symlink.")
            if path.exists() and sha256_file(path) != digest:
                raise ProbeError("Existing content-addressed snapshot is corrupt; remove it manually.")
            if not path.exists():
                raw_bin.rename(path)
            if args.keep_archive and archive.exists():
                archive.rename(cache / f"{digest}.tar.gz")
            meta = {"source": url, "filename": path.name, "sha256": digest,
                    "downloaded_at": utcnow(), "network": args.network}
            atomic_json(manifest_path, meta)
    say(f"Snapshot: {path}\nCalculating SHA-256...")
    digest = sha256_file(path)
    expected = args.snapshot_sha256 or meta.get("sha256")
    if expected and digest.lower() != expected.lower():
        raise ProbeError("Snapshot SHA-256 mismatch. No peer was tested.")
    say(f"SHA-256: {digest}")
    return path, digest, meta


def clean_child_env() -> dict[str, str]:
    env = os.environ.copy()
    for key in ("LD_PRELOAD", "LD_AUDIT", "PROXYCHAINS_CONF_FILE", "PROXYCHAINS_QUIET_MODE"):
        env.pop(key, None)
    return env


def run_command(command: list[str], timeout: float = 30) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(command, text=True, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, timeout=timeout, env={**clean_child_env(), "LC_ALL":"C"})
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ProbeError(f"Cannot run {shlex.join(command)}: {exc}") from exc


def install_system_helpers(yes: bool) -> None:
    missing = []
    if not shutil.which("dpkg-deb"):
        missing.append("dpkg")
    if not shutil.which("gpg"):
        missing.append("gnupg")
    cert = ssl.get_default_verify_paths().cafile
    if not cert or not Path(cert).is_file():
        missing.append("ca-certificates")
    if not missing:
        return
    if not shutil.which("apt-get"):
        raise ProbeError("Install dpkg, GnuPG and CA certificates manually on this distribution.")
    prefix = [] if os.geteuid() == 0 else ["sudo"]
    if prefix and not shutil.which("sudo"):
        raise ProbeError("sudo is not installed. Ask an administrator to install: " + " ".join(missing))
    commands = [prefix + ["apt-get", "update"],
                prefix + ["apt-get", "install", "--no-install-recommends", *missing]]
    if not ask_permission("Missing installation helpers. Proposed system commands:\n  " +
                          "\n  ".join(shlex.join(x) for x in commands), yes):
        raise ProbeError("Dependency installation was not authorized.")
    for command in commands:
        if command[-1] != "update" and yes:
            command.insert(command.index("install") + 1, "-y")
        if subprocess.run(command, check=False).returncode:
            raise ProbeError("Package helper installation failed. No nodeos was installed.")


def install_runtime_dependencies(package: Path, yes: bool) -> None:
    """Offer the signed package's runtime dependencies, refusing a non-additive plan."""
    depends = run_command(['dpkg-deb', '-f', str(package), 'Depends'])
    requirements = depends.stdout.strip()
    if (depends.returncode or not requirements or requirements.startswith('-') or
            not re.fullmatch(r'[A-Za-z0-9.+:~(),|<>= _/\-]+', requirements)):
        raise ProbeError('Cannot safely read the Leap runtime dependency list; install libraries manually.')
    if not shutil.which('apt-get'):
        raise ProbeError(f'Runtime libraries required: {requirements}. Install them manually.')
    command = ['apt-get', '--no-remove', '--no-install-recommends', 'satisfy', requirements]
    preview = run_command(command[:1] + ['--simulate'] + command[1:])
    if (preview.returncode or re.search(r'^Remv ', preview.stdout, re.M) or
            re.search(r'^Inst \S+ \[', preview.stdout, re.M)):
        raise ProbeError('Runtime dependency installation needs manual review (resolution failure, '
                         'removal or upgrade in apt plan). No changes applied.\n' + preview.stdout[-4000:])
    prefix = [] if os.geteuid() == 0 else ['sudo']
    if prefix and not shutil.which('sudo'):
        raise ProbeError(f'sudo unavailable. Ask an administrator to install: {requirements}')
    say(preview.stdout)
    if not ask_permission('Proposed runtime-library installation:\n  ' + shlex.join(prefix + command) +
                          '\nOnly the dependency packages are installed; nodeos stays private to the workspace.', yes):
        raise ProbeError('Runtime library installation not approved.')
    # The apt transaction still performs its own dependency checks; never allow package removal.
    actual = prefix + command
    if yes:
        actual.insert(len(prefix) + 1, '-y')
    if subprocess.run(actual, check=False, env={**os.environ, 'LC_ALL':'C'}).returncode:
        raise ProbeError('Runtime library installation failed; no nodeos was installed system-wide.')


def install_local_nodeos(root: Path, args: argparse.Namespace) -> Path:
    os_release = {}
    try:
        for line in Path('/etc/os-release').read_text().splitlines():
            if '=' in line:
                k, v = line.split('=', 1)
                os_release[k] = v.strip('"')
    except OSError:
        pass
    if platform.machine() not in ("x86_64", "amd64"):
        raise ProbeError("Automatic Leap installation supports amd64 only. Supply --nodeos-bin for this architecture.")
    if os_release.get("ID") != "ubuntu" or os_release.get("VERSION_ID") not in ("22.04", "24.04"):
        raise ProbeError("Automatic Leap installation is limited to Ubuntu 22.04/24.04 amd64. "
                         "On other systems install a compatible Leap manually and use --nodeos-bin.")
    message = (f"nodeos is missing. Download the pinned official Leap {LEAP_VERSION} package,\n"
               "verify its detached signature, and extract it into this workspace?\n"
               "This does NOT install/replace /usr/bin/nodeos or run package maintainer scripts.\n"
               f"Source: {LEAP_URL}\nDestination: {root / 'tools'}")
    if not ask_permission(message, args.yes and args.install_deps):
        raise ProbeError("Install Leap manually, or use --install-deps (and --yes for unattended approval).")
    install_system_helpers(args.yes and args.install_deps)
    tools = root / 'tools'
    tools.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.leap-', dir=tools) as temp_name:
        stage = Path(temp_name)
        package = stage / 'leap.deb'
        signature = stage / 'leap.deb.asc'
        download(LEAP_URL, package, 256 * 1024 ** 2)
        digest = sha256_file(package)
        if args.nodeos_sha256 and digest != args.nodeos_sha256.lower():
            raise ProbeError("Leap package SHA-256 does not match --nodeos-sha256.")
        download(LEAP_URL + '.asc', signature, 1024 ** 2)
        keyring = stage / 'gnupg'
        keyring.mkdir(mode=0o700)
        for i, url in enumerate(MAINTAINER_KEYS):
            key = stage / f'maintainer-{i}.gpg'
            download(url, key, 4 * 1024 ** 2)
            result = run_command(['gpg', '--batch', '--homedir', str(keyring), '--import', str(key)])
            if result.returncode:
                raise ProbeError('Cannot import release verification keys: ' + result.stdout[-2000:])
        verification = run_command(['gpg', '--batch', '--homedir', str(keyring), '--status-fd=1',
                                    '--verify', str(signature), str(package)])
        fingerprints = re.findall(r'\[GNUPG:\] VALIDSIG ([0-9A-F]+)', verification.stdout)
        if verification.returncode or not fingerprints:
            raise ProbeError('Leap release signature verification failed: ' + verification.stdout[-3000:])
        say('Verified release signer(s): ' + ', '.join(fingerprints))
        # Extraction only: dpkg-deb never executes maintainer scripts.
        extracted = stage / 'root'
        result = run_command(['dpkg-deb', '-x', str(package), str(extracted)], timeout=120)
        if result.returncode:
            raise ProbeError('Package extraction failed: ' + result.stdout[-2000:])
        candidates = [p for p in extracted.rglob('nodeos') if p.is_file() and os.access(p, os.X_OK)]
        if len(candidates) != 1:
            raise ProbeError('Unexpected Leap package layout: could not identify exactly one nodeos executable.')
        executable = candidates[0]
        result = run_command([str(executable), '--version'])
        if result.returncode and ('error while loading shared libraries' in result.stdout.lower() or
                                  'cannot open shared object file' in result.stdout.lower()):
            install_runtime_dependencies(package, args.yes and args.install_deps)
            result = run_command([str(executable), '--version'])
        if result.returncode:
            deps = run_command(['dpkg-deb', '-f', str(package), 'Depends']).stdout.strip()
            raise ProbeError('Extracted Leap cannot execute on this host. System libraries may be missing.\n'
                             f'Package Depends: {deps}\n{result.stdout[-2000:]}\n'
                             'Install the appropriate runtime libraries through your distribution; no production binary changed.')
        target = tools / f'leap-{LEAP_VERSION}-{digest[:16]}'
        if target.exists():
            raise ProbeError(f'Tool destination already exists: {target}. Inspect it before retrying.')
        relative_executable = executable.relative_to(extracted)
        extracted.rename(target)
        final = target / relative_executable
        atomic_json(tools / 'nodeos.json', {'path': str(final.resolve()), 'version': LEAP_VERSION,
                    'source': LEAP_URL, 'package_sha256': digest, 'signers': fingerprints, 'installed_at': utcnow()})
        return final


def ensure_nodeos(root: Path, args: argparse.Namespace) -> tuple[Path, str]:
    if args.nodeos_bin:
        supplied = shutil.which(args.nodeos_bin) or str(Path(args.nodeos_bin).expanduser())
        candidate = Path(supplied)
        if not candidate.is_file() or not os.access(candidate, os.X_OK):
            raise ProbeError(f'--nodeos-bin is not executable: {candidate}')
    elif shutil.which('nodeos'):
        candidate = Path(shutil.which('nodeos'))
    else:
        candidate = None
        installed = root / 'tools' / 'nodeos.json'
        if installed.exists():
            try:
                possible = Path(json.loads(installed.read_text())['path'])
                if possible.is_file() and possible.resolve().is_relative_to((root / 'tools').resolve()):
                    candidate = possible
            except (ValueError, KeyError, OSError):
                pass
        if candidate is None:
            if not args.install_deps and (args.check or not sys.stdin.isatty()):
                raise ProbeError('nodeos is missing. Run ./setup.sh or add --install-deps; '
                                 'unattended installation also needs --yes. No network tests started.')
            candidate = install_local_nodeos(root, args)
    candidate = candidate.resolve()
    result = run_command([str(candidate), '--full-version'])
    if result.returncode:
        result = run_command([str(candidate), '--version'])
    if result.returncode:
        raise ProbeError(f'nodeos cannot execute: {result.stdout[-2000:]}')
    version = result.stdout.strip()
    # --full-version normally returns a build suffix, e.g.
    # v5.0.3-d133c6413ce8ce2e96096a0513ec25b4a8dbe837. Compare the
    # semantic version, not the complete build string. A leading 'v' is not a
    # regex word boundary, so the previous \b5\.0\.3\b check incorrectly
    # warned for the normal official version string.
    selected_semver = extract_semver(version)
    if selected_semver != LEAP_VERSION:
        say(f'WARNING: this probe was designed around Leap {LEAP_VERSION}; selected version: {version}')
    help_result = run_command([str(candidate), '--help'])
    for option in ('--snapshot', '--data-dir', '--config-dir'):
        if help_result.returncode or option not in help_result.stdout:
            raise ProbeError(f'Selected executable does not advertise {option}: {candidate}')
    return candidate, version


def receive_exact(sock: socket.socket, count: int, deadline: float) -> bytes:
    chunks = bytearray()
    while len(chunks) < count:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ProxyError('SOCKS5 negotiation timed out.')
        sock.settimeout(remaining)
        chunk = sock.recv(count - len(chunks))
        if not chunk:
            raise ProxyError('SOCKS5 closed during negotiation.')
        chunks.extend(chunk)
    return bytes(chunks)


def open_socks(proxy: tuple[str, int], target: tuple[str, int] | None, timeout: float,
               credentials: tuple[str, str] | None = None) -> socket.socket:
    """RFC 1928/1929. No handshake/header bytes leak into relayed P2P data."""
    deadline = time.monotonic() + timeout
    s = socket.create_connection(proxy, timeout=timeout)
    try:
        method = 2 if credentials else 0
        s.sendall(bytes((5, 1, method)))
        if receive_exact(s, 2, deadline) != bytes((5, method)):
            raise ProxyError('SOCKS5 authentication method rejected.')
        if credentials:
            user, password = (x.encode('utf-8') for x in credentials)
            if not 1 <= len(user) <= 255 or not 1 <= len(password) <= 255:
                raise ProxyError('SOCKS credentials must each encode to 1..255 bytes.')
            s.sendall(bytes((1, len(user))) + user + bytes((len(password),)) + password)
            if receive_exact(s, 2, deadline) != b'\x01\x00':
                raise ProxyError('SOCKS5 username/password authentication rejected.')
        if target is None:
            return s  # Preflight checks method/auth only, not remote P2P.
        host, port = target
        try:
            address = ipaddress.ip_address(host)
            encoded = bytes((1 if address.version == 4 else 4,)) + address.packed
        except ValueError:
            host_bytes = host.encode('idna')
            if len(host_bytes) > 255:
                raise ProxyError('SOCKS5 hostname too long.')
            encoded = bytes((3, len(host_bytes))) + host_bytes
        s.sendall(b'\x05\x01\x00' + encoded + struct.pack('!H', port))
        head = receive_exact(s, 4, deadline)
        if head[0] != 5 or head[2] != 0:
            raise ProxyError('Malformed SOCKS5 CONNECT reply.')
        if head[1] != 0:
            names = {1:'general failure', 2:'ruleset denied', 3:'network unreachable',
                     4:'host unreachable', 5:'connection refused', 6:'TTL expired',
                     7:'command unsupported', 8:'address type unsupported'}
            raise ProxyError(f'SOCKS5 CONNECT rejected: REP={head[1]} ({names.get(head[1], "unknown")}).')
        if head[3] == 1:
            receive_exact(s, 4, deadline)
        elif head[3] == 4:
            receive_exact(s, 16, deadline)
        elif head[3] == 3:
            receive_exact(s, receive_exact(s, 1, deadline)[0], deadline)
        else:
            raise ProxyError('Malformed SOCKS5 bound address type.')
        receive_exact(s, 2, deadline)
        s.settimeout(None)
        return s
    except BaseException:
        s.close()
        raise


class NativeRelay:
    """One local TCP connection; bounded buffers; backpressure and half-close support."""
    BUFFER_LIMIT = 512 * 1024

    def __init__(self, port: int, proxy: tuple[str, int], target: tuple[str, int],
                 timeout: float, credentials: tuple[str, str] | None = None):
        self.port, self.proxy, self.target = port, proxy, target
        self.timeout, self.credentials = timeout, credentials
        self.stop_event = threading.Event()
        self.server: socket.socket | None = None
        self.thread: threading.Thread | None = None
        self.sockets: list[socket.socket] = []
        self.errors: list[str] = []
        self.stats = {'connections': 0, 'socks_connected': False,
                      'nodeos_to_peer_bytes': 0, 'peer_to_nodeos_bytes': 0}

    def start(self) -> str:
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(('127.0.0.1', self.port))
        self.port = self.server.getsockname()[1]
        self.server.listen(1)
        self.server.settimeout(0.2)
        self.thread = threading.Thread(target=self._serve, name='probe-native-relay', daemon=True)
        self.thread.start()
        return f'127.0.0.1:{self.port}'

    def _serve(self) -> None:
        try:
            while not self.stop_event.is_set():
                try:
                    client, _ = self.server.accept()
                    break
                except socket.timeout:
                    continue
            else:
                return
            self.server.close()  # No reconnect storm against the upstream peer.
            self.sockets.append(client)
            self.stats['connections'] += 1
            upstream = open_socks(self.proxy, self.target, self.timeout, self.credentials)
            self.sockets.append(upstream)
            self.stats['socks_connected'] = True
            self._pump(client, upstream)
        except Exception as exc:
            if not self.stop_event.is_set():
                self.errors.append(str(exc))
        finally:
            for s in self.sockets:
                with contextlib.suppress(OSError):
                    s.close()

    def _pump(self, client: socket.socket, upstream: socket.socket) -> None:
        pair = {client: upstream, upstream: client}
        buffers = {client: bytearray(), upstream: bytearray()}  # Waiting to SEND on this socket.
        eof = {client: False, upstream: False}
        write_closed = {client: False, upstream: False}
        for s in pair:
            s.setblocking(False)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        with selectors.DefaultSelector() as selector:
            while not self.stop_event.is_set():
                if all(eof.values()) and not any(buffers.values()):
                    return
                for s, other in pair.items():
                    if eof[other] and not buffers[s] and not write_closed[s]:
                        with contextlib.suppress(OSError):
                            s.shutdown(socket.SHUT_WR)
                        write_closed[s] = True
                    mask = 0
                    if not eof[s] and len(buffers[other]) < self.BUFFER_LIMIT:
                        mask |= selectors.EVENT_READ
                    if buffers[s] and not write_closed[s]:
                        mask |= selectors.EVENT_WRITE
                    with contextlib.suppress(KeyError):
                        selector.unregister(s)
                    if mask:
                        selector.register(s, mask)
                for key, mask in selector.select(0.2):
                    s, other = key.fileobj, pair[key.fileobj]
                    if mask & selectors.EVENT_READ:
                        try:
                            data = s.recv(min(65536, self.BUFFER_LIMIT - len(buffers[other])))
                        except BlockingIOError:
                            continue
                        if data:
                            buffers[other].extend(data)
                        else:
                            eof[s] = True
                    if mask & selectors.EVENT_WRITE:
                        try:
                            count = s.send(buffers[s])
                        except BlockingIOError:
                            continue
                        if count == 0:
                            raise ConnectionError('Zero-length relay write.')
                        del buffers[s][:count]
                        direction = 'nodeos_to_peer_bytes' if s is upstream else 'peer_to_nodeos_bytes'
                        self.stats[direction] += count

    def stop(self) -> None:
        self.stop_event.set()
        if self.server:
            with contextlib.suppress(OSError):
                self.server.close()
        for s in self.sockets:
            with contextlib.suppress(OSError):
                s.shutdown(socket.SHUT_RDWR)
        if self.thread:
            self.thread.join(timeout=self.timeout + 1)
            if self.thread.is_alive():
                raise ProbeError('Native relay did not stop; refusing to continue with another peer.')


class LogTail:
    def __init__(self, path: Path):
        self.path, self.offset = path, 0
        self.evidence: list[str] = []
        self.remote_reason = ''

    def read(self) -> str | None:
        try:
            with self.path.open('rb') as f:
                f.seek(self.offset)
                data = f.read()
                self.offset = f.tell()
        except OSError:
            return None
        text = data.decode('utf-8', errors='replace')
        classification = None
        for line in text.splitlines():
            low = line.lower()
            label = None
            if 'wrong chain' in low:
                label = 'WRONG_CHAIN'
            elif 'go_away_message' in low:
                match = re.search(r'reason\s*=\s*(.*)', line)
                if match:
                    self.remote_reason = match.group(1).strip()
                label = 'PEER_CLOSED'
            elif 'peer closed connection' in low:
                label = 'PEER_CLOSED'
            elif 'connection refused' in low or 'connection failed' in low:
                label = 'CONNECT_FAIL'
            elif 'heartbeat timed out' in low:
                label = 'PEER_CLOSED'
            if label or 'sync_reassign_fetch' in low or 'unable to continue syncing' in low:
                if len(self.evidence) < 30:
                    self.evidence.append(line[-1600:])
            if label == 'WRONG_CHAIN' or classification is None:
                classification = label or classification
        return classification


def owns_listening_port(pid: int, port: int) -> bool:
    """Verify the local API listener belongs to OUR child before sending control RPCs."""
    try:
        inodes = set()
        for fd in Path(f'/proc/{pid}/fd').iterdir():
            with contextlib.suppress(OSError):
                link = os.readlink(fd)
                match = re.fullmatch(r'socket:\[(\d+)\]', link)
                if match:
                    inodes.add(match.group(1))
        for line in Path('/proc/net/tcp').read_text().splitlines()[1:]:
            fields = line.split()
            if (len(fields) > 9 and fields[1] == f'0100007F:{port:04X}' and
                    fields[3] == '0A' and fields[9] in inodes):
                return True
    except OSError:
        return False
    return False


class Nodeos:
    def __init__(self, executable: Path, args: argparse.Namespace, run_dir: Path):
        self.executable, self.args, self.run_dir = executable, args, run_dir
        self.process: subprocess.Popen | None = None
        self.log_handle = None
        self.log_path: Path | None = None
        self.marker = run_dir / 'nodeos.pid.json'

    def start(self, data_dir: Path, config_dir: Path, log_path: Path, snapshot: Path) -> None:
        if self.process is not None:
            raise ProbeError('Diagnostic nodeos is already owned by this probe.')
        data_dir.mkdir(parents=True, exist_ok=False)
        config_dir.mkdir(parents=True, exist_ok=False)
        config = f'''agent-name = XPR-PeerProbe/{VERSION}
http-server-address = 127.0.0.1:{self.args.http_port}
p2p-listen-endpoint = 127.0.0.1:{self.args.p2p_port}
database-map-mode = mapped_private
chain-state-db-size-mb = {self.args.chain_db_mb}
chain-threads = {self.args.chain_threads}
http-threads = 2
http-validate-host = true
verbose-http-errors = true
http-max-response-time-ms = 1000
sync-fetch-span = {self.args.sync_fetch_span}
contracts-console = false
plugin = eosio::chain_plugin
plugin = eosio::chain_api_plugin
plugin = eosio::net_api_plugin
'''
        (config_dir / 'config.ini').write_text(config)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path = log_path
        self.log_handle = log_path.open('xb')
        cmd = [str(self.executable), '--data-dir', str(data_dir), '--config-dir', str(config_dir),
               '--snapshot', str(snapshot)]
        try:
            self.process = subprocess.Popen(cmd, stdout=self.log_handle, stderr=subprocess.STDOUT,
                                            start_new_session=True, env=clean_child_env())
            atomic_json(self.marker, {'pid': self.process.pid, 'data_dir': str(data_dir),
                                     'command': cmd, 'started_at': utcnow()})
        except BaseException:
            if self.process is None:
                self.log_handle.close()
                self.log_handle = None
            raise

    def api(self, route: str, body: Any = None, timeout: float | None = None) -> Any:
        if self.process and self.process.poll() is not None:
            raise ProbeError(f'Diagnostic nodeos exited ({self.process.returncode}). See {self.log_path}')
        conn = http.client.HTTPConnection('127.0.0.1', self.args.http_port,
                                          timeout=timeout or self.args.api_timeout)
        try:
            encoded = json.dumps({} if body is None else body)
            conn.request('POST', route, body=encoded.encode(), headers={'Content-Type':'application/json'})
            response = conn.getresponse()
            raw = response.read(4 * 1024 ** 2 + 1)
            if len(raw) > 4 * 1024 ** 2:
                raise ProbeError('Unexpectedly large local nodeos API response.')
            # Leap's HTTP plugin may return any successful 2xx status for local
            # plugin RPCs. In particular, Leap 5.0.3 can return HTTP 201 for
            # /v1/net/connections with a perfectly valid JSON body. Treat the
            # whole 2xx class as success instead of requiring exactly HTTP 200.
            if not 200 <= response.status < 300:
                raise ProbeError(f'Local RPC {route}: HTTP {response.status}: {raw[:1200]!r}')
            return json.loads(raw) if raw else None
        except (OSError, http.client.HTTPException, json.JSONDecodeError) as exc:
            raise ProbeError(f'Local RPC {route} failed: {exc}') from exc
        finally:
            conn.close()

    def wait_ready(self) -> dict:
        deadline = time.monotonic() + self.args.api_start_timeout
        last = ''
        next_notice = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise ProbeError(f'nodeos exited during restore ({self.process.returncode}). See {self.log_path}')
            try:
                if not owns_listening_port(self.process.pid, self.args.http_port):
                    raise ProbeError('Waiting for an HTTP listener owned by the diagnostic nodeos process.')
                info = self.api('/v1/chain/get_info', timeout=min(self.args.api_timeout, max(.1, deadline-time.monotonic())))
                if not isinstance(info, dict) or 'head_block_num' not in info:
                    raise ProbeError('Invalid get_info reply.')
                peers = self.api('/v1/net/connections')
                if not isinstance(peers, list) or peers:
                    raise ProbeError('Unexpected existing peers on diagnostic node; stopping.')
                return info
            except ProbeError as exc:
                last = str(exc)
            if time.monotonic() > next_notice:
                say('  Restoring snapshot; waiting for local API...')
                next_notice = time.monotonic() + 10
            time.sleep(.2)
        raise ProbeError(f'Local API startup timeout: {last}. See {self.log_path}')

    def disconnect(self, endpoint: str) -> None:
        self.api('/v1/net/disconnect', endpoint)

    def stop(self) -> None:
        process = self.process
        if process is None:
            return
        if process.poll() is None:
            # Signal only the process group we created, never pidof/pkill nodeos.
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=self.args.shutdown_timeout)
            except subprocess.TimeoutExpired as exc:
                raise ProbeError(f'PID {process.pid} did not stop gracefully. No SIGKILL was sent, '
                                 f'and data was preserved. Inspect {self.marker}') from exc
        if self.log_handle:
            self.log_handle.close()
            self.log_handle = None
        self.process = None
        self.marker.unlink(missing_ok=True)


def validate_snapshot_state(info: dict, network: str, expected: dict | None) -> dict:
    meta = {'chain_id': info.get('chain_id'), 'head_block_num': int(info['head_block_num']),
            'last_irreversible_block_num': int(info['last_irreversible_block_num']),
            'head_block_id': info.get('head_block_id'), 'head_block_time': info.get('head_block_time')}
    if meta['chain_id'] != NETWORKS[network]['chain_id']:
        raise ProbeError(f'LOCAL SNAPSHOT NETWORK MISMATCH: --network {network} expects '
                         f'{NETWORKS[network]["chain_id"]}, snapshot reports {meta["chain_id"]}. '
                         'Do not classify remote peers; choose the correct snapshot.')
    if meta['head_block_num'] != meta['last_irreversible_block_num']:
        raise ProbeError('Snapshot did not restore to HEAD == LIB; cannot make an unambiguous catch-up benchmark.')
    if expected:
        for key in ('chain_id', 'head_block_num', 'last_irreversible_block_num', 'head_block_id'):
            if meta[key] != expected[key]:
                raise ProbeError(f'Snapshot restored to a different {key}; comparison stopped.')
    return meta


def valid_handshake(status: Any) -> dict | None:
    if status is None:
        return None
    if not isinstance(status, dict):
        raise ProbeError('Local net/status did not return an object or null.')
    # Leap 5.0.3 exposes is_socket_open. Never use a retained handshake from a closed socket.
    if status.get('connecting') or status.get('is_socket_open') is False:
        return None
    handshake = status.get('last_handshake')
    if (isinstance(handshake, dict) and handshake.get('chain_id') and
            int(handshake.get('head_num', 0)) > 0):
        return handshake
    return None


def new_result(endpoint: str, mode: str, phase: str, round_number: int) -> dict:
    return {'endpoint': endpoint, 'network_mode': mode, 'phase': phase, 'round': round_number,
            'started_at': utcnow(), 'classification': 'NOT_RUN', 'reason': '',
            'handshake_ok': False, 'handshake_ms': None, 'remote_head': None, 'remote_lib': None,
            'lag': None, 'available_irreversible_blocks': None, 'gained_blocks': 0,
            'target_blocks': 0, 'blocks_per_second': 0.0, 'elapsed_seconds': 0.0,
            'resolved_ips': [], 'selected_ip': None, 'connected_ip': None,
            'relay': None, 'evidence': [], 'remote_go_away_reason': '', 'completed_target': False}


def run_sample(node: Nodeos, args: argparse.Namespace, run_dir: Path, snapshot: Path,
               baseline: dict | None, row: dict, target: tuple[str, int],
               proxy: tuple[str, int] | None, credentials: tuple[str, str] | None,
               identifier: str) -> dict:
    data_dir = run_dir / 'data' / identifier
    config_dir = run_dir / 'configs' / identifier
    log_path = run_dir / 'logs' / f'{identifier}.log'
    row['log'] = str(log_path.relative_to(run_dir))
    relay = None
    connect_endpoint = endpoint_text(*target)
    connected = False
    tail = LogTail(log_path)
    try:
        restore_start = time.monotonic()
        node.start(data_dir, config_dir, log_path, snapshot)
        initial = node.wait_ready()
        row['restore_seconds'] = time.monotonic() - restore_start
        meta = validate_snapshot_state(initial, args.network, baseline)
        row.update(start_head=meta['head_block_num'], start_lib=meta['last_irreversible_block_num'],
                   snapshot_head_equals_lib=True)
        if baseline is None:
            say(f'  Snapshot HEAD/LIB: {meta["head_block_num"]}/{meta["last_irreversible_block_num"]}; '
                f'block time: {meta["head_block_time"]}')
        if row['network_mode'] == 'NATIVE_SOCKS5':
            relay = NativeRelay(args.relay_port, proxy, target, args.proxy_timeout, credentials)
            connect_endpoint = relay.start()
        row['connect_endpoint'] = connect_endpoint
        # Ignore initialization messages. From this point the log belongs to this single peer attempt.
        tail.read()
        tail.evidence.clear()
        started = time.monotonic()
        node.api('/v1/net/connect', connect_endpoint)
        connected = True
        handshake = None
        while time.monotonic() - started < args.handshake_timeout:
            terminal = tail.read()
            if relay and relay.errors:
                row.update(classification='PROXY_ERROR', reason='; '.join(relay.errors))
                return meta
            if terminal:
                row.update(classification=terminal, reason='nodeos log: remote transport/protocol event')
                return meta
            status = node.api('/v1/net/status', connect_endpoint)
            handshake = valid_handshake(status)
            if handshake:
                if handshake['chain_id'] != meta['chain_id']:
                    row.update(classification='WRONG_CHAIN', reason='Received a different chain_id',
                               remote_chain_id=handshake['chain_id'])
                    return meta
                row.update(handshake_ok=True, handshake_ms=(time.monotonic()-started)*1000,
                           connected_ip=status.get('remote_ip'), agent=handshake.get('agent', ''),
                           remote_head=int(handshake['head_num']),
                           remote_lib=int(handshake.get('last_irreversible_block_num', 0)))
                break
            time.sleep(args.poll_interval)
        if not handshake:
            row.update(classification='SILENT', reason='No valid Antelope handshake before timeout; '
                       'TCP success is not implied by this label alone.')
            return meta
        start_head = meta['head_block_num']
        row['lag'] = row['remote_head'] - start_head
        available = max(0, row['remote_lib'] - start_head)
        row['available_irreversible_blocks'] = available
        if row['phase'] == 'A':
            until = time.monotonic() + args.phase_a_hold
            while time.monotonic() < until:
                terminal = tail.read()
                status = node.api('/v1/net/status', connect_endpoint)
                if (relay and relay.errors) or terminal or not valid_handshake(status):
                    row.update(classification='PROXY_ERROR' if relay and relay.errors else terminal or 'PEER_CLOSED',
                               reason='Connection did not remain valid during Phase A hold.')
                    return meta
                time.sleep(args.poll_interval)
            row.update(classification='HANDSHAKE_OK', reason='Handshake/short hold only, not a catch-up benchmark.')
            return meta
        row['target_blocks'] = args.catchup_blocks
        if row['lag'] <= 0:
            row.update(classification='NO_HISTORY', reason='Peer head is not ahead of the snapshot.')
            return meta
        if available < args.catchup_blocks:
            row.update(classification='INSUFFICIENT_HISTORY', reason=(
                f'Only {available} irreversible blocks ahead at handshake; need {args.catchup_blocks}. '
                'Use an older snapshot or a deliberately smaller target.'))
            return meta
        last_head = start_head
        last_progress = time.monotonic()
        row['classification'] = 'SLOW'
        row['reason'] = 'Catch-up deadline expired.'
        while True:
            # Throughput numerator and denominator come from the SAME observation.
            info = node.api('/v1/chain/get_info')
            observed = time.monotonic()
            current_head = int(info['head_block_num'])
            if current_head < last_head:
                raise ProbeError('Local HEAD regressed during irreversible catch-up; inspect the nodeos log.')
            if current_head > last_head:
                last_head, last_progress = current_head, observed
            gained = max(0, current_head - start_head)
            row.update(gained_blocks=gained, elapsed_seconds=observed-started,
                       blocks_per_second=gained/max(.001, observed-started), end_head=current_head)
            if gained >= args.catchup_blocks:
                on_time = observed - started <= args.catchup_timeout
                row.update(classification='GOOD' if on_time else 'SLOW', completed_target=True,
                           reason=('Full requested catch-up target completed within the test budget.' if on_time
                                   else 'Full target was observed only after the configured time budget.'))
                break
            terminal = tail.read()
            if relay and relay.errors:
                row.update(classification='PROXY_ERROR', reason='; '.join(relay.errors))
                break
            if terminal:
                row.update(classification=terminal, reason='Connection/protocol event during catch-up.')
                break
            status = node.api('/v1/net/status', connect_endpoint)
            if not valid_handshake(status):
                row.update(classification='PEER_CLOSED', reason='Previously valid P2P connection is no longer open.')
                break
            if observed - started >= args.catchup_timeout:
                break
            if observed - last_progress >= args.stall_seconds:
                row['reason'] = f'No HEAD progress for {args.stall_seconds:g}s.'
                break
            time.sleep(args.poll_interval)
        return meta
    except ProbeError as exc:
        row.update(classification='LOCAL_ERROR', reason=str(exc))
        raise
    finally:
        # Cleanup is mandatory on success, Ctrl+C and exceptions. Preserve data if SIGTERM fails.
        if 'started' in locals() and row['elapsed_seconds'] == 0:
            row['elapsed_seconds'] = time.monotonic() - started
        if connected and node.process and node.process.poll() is None:
            try:
                node.disconnect(connect_endpoint)
            except ProbeError as exc:
                row['disconnect_warning'] = str(exc)
        try:
            node.stop()
        finally:
            if relay:
                relay.stop()
                row['relay'] = {**relay.stats, 'errors': list(relay.errors)}
        tail.read()
        row['evidence'] = tail.evidence
        row['remote_go_away_reason'] = tail.remote_reason
        allocated, apparent = tree_size(data_dir)
        row.update(data_disk_bytes_after_stop=allocated, data_apparent_bytes_after_stop=apparent,
                   finished_at=utcnow())
        if not args.keep_temp and node.process is None:
            remove_owned_data(data_dir, run_dir)


def sorted_rows(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda x: (x['phase'], x['network_mode'], x['classification'] != 'GOOD',
                                      -x['blocks_per_second'], x['endpoint'], x['round']))


def recommendation_rows(rows: list[dict], mode: str, rounds: int, limit: int) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        if row['phase'] == 'B' and row['network_mode'] == mode:
            grouped.setdefault(row['endpoint'], []).append(row)
    candidates = []
    for endpoint, samples in grouped.items():
        if len(samples) == rounds and all(r['classification'] == 'GOOD' for r in samples):
            speeds = sorted(r['blocks_per_second'] for r in samples)
            median = (speeds[(len(speeds)-1)//2] + speeds[len(speeds)//2])/2
            candidates.append({'endpoint': endpoint, 'median_blocks_per_second': median})
    return sorted(candidates, key=lambda x: -x['median_blocks_per_second'])[:limit]


def write_reports(run_dir: Path, report: dict, args: argparse.Namespace) -> None:
    results = run_dir / 'results'
    results.mkdir(exist_ok=True)
    report['updated_at'] = utcnow()
    atomic_json(results / 'results.json', report)
    fields = ['endpoint','network_mode','phase','round','classification','reason','selected_ip',
              'connected_ip','connect_endpoint','handshake_ok','handshake_ms','start_head','start_lib',
              'remote_head','remote_lib','lag','available_irreversible_blocks','target_blocks',
              'gained_blocks','elapsed_seconds','blocks_per_second','completed_target',
              'restore_seconds','data_disk_bytes_after_stop','data_apparent_bytes_after_stop',
              'remote_go_away_reason','resolved_ips','relay','log','started_at','finished_at']
    for phase in ('A', 'B'):
        path = results / f'phase-{phase.lower()}.csv'
        temp = path.with_suffix('.csv.tmp')
        with temp.open('w', newline='', encoding='utf-8') as out:
            writer = csv.DictWriter(out, fields, extrasaction='ignore')
            writer.writeheader()
            for row in sorted_rows(report['samples']):
                if row['phase'] != phase:
                    continue
                prepared = {k: (json.dumps(v, ensure_ascii=False) if isinstance(v, (dict,list)) else v)
                            for k, v in row.items()}
                writer.writerow(prepared)
        os.replace(temp, path)
    lines = [f'# XPR {args.network.title()} P2P report', '',
             f'Run: `{report["run_id"]}`  |  State: **{report["state"]}**',
             f'Probe: `{VERSION}`  |  nodeos: `{report.get("nodeos_version", "unknown")}`',
             f'Snapshot SHA-256: `{report.get("snapshot_sha256", "unknown")}`', '',
             'Point-in-time results from this source IP/route. Not a network-wide uptime or fault verdict.',
             'GOOD means the full configured target completed; speeds include local block application.', '',
             '| Network path | Endpoint | Result | Blocks/target | blk/s | lag (blocks) | hs (ms) |',
             '|---|---|---|---:|---:|---:|---:|']
    for row in sorted_rows(report['samples']):
        if row['phase'] != 'B':
            continue
        hs = '-' if row['handshake_ms'] is None else f'{row["handshake_ms"]:.1f}'
        lag = '-' if row['lag'] is None else str(row['lag'])
        lines.append(f'| {row["network_mode"]} | `{row["endpoint"]}` | {row["classification"]} | '
                     f'{row["gained_blocks"]}/{row["target_blocks"]} | '
                     f'{row["blocks_per_second"]:.1f} | {lag} | {hs} |')
    lines += ['', 'PEER_CLOSED may reflect slot limits, duplicate/source-IP policy, overload, or another rejection.',
              'SILENT alone does not establish a successful TCP connection. PROXY_ERROR/LOCAL_ERROR are not peer verdicts.',
              'For WRONG_CHAIN consult the evidence: a received chain ID and a remote go_away reason are distinguished.',
              'See results.json for timings, reason, raw log evidence and relay counters.', '']
    (results / 'report.md').write_text('\n'.join(lines), encoding='utf-8')
    direct = recommendation_rows(report['samples'], 'DIRECT', args.rounds, args.recommend)
    conf = ['# Point-in-time DIRECT-route candidates only; no production config is modified.',
            '# Require GOOD in every requested Phase B round. No inference of global health.']
    for row in direct:
        conf += [f'# Median {row["median_blocks_per_second"]:.1f} blk/s',
                 f'p2p-peer-address = {row["endpoint"]}']
    if not direct:
        conf.append('# No completed DIRECT candidates in this run.')
    (results / 'p2p-peers.conf').write_text('\n'.join(conf) + '\n')
    proxy = recommendation_rows(report['samples'], 'NATIVE_SOCKS5', args.rounds, args.recommend)
    (results / 'proxy-candidates.txt').write_text(
        '# Passed via SOCKS5 only. Do not interpret this as DIRECT reachability.\n' +
        '\n'.join(r['endpoint'] for r in proxy) + '\n')
    storage = report.get('storage', {})
    (results / 'storage.txt').write_text('\n'.join(
        f'{k}: {human_bytes(v) if isinstance(v,(int,float)) and k.endswith("bytes") else v}'
        for k,v in storage.items()) + '\n')
    atomic_json(run_dir.parent.parent / 'latest.json',
                {'run_id': report['run_id'], 'state': report['state'],
                 'run_dir': str(run_dir), 'results_dir': str(results)})


def print_ranking(rows: list[dict]) -> None:
    say('\n=== Final ranking (Phase B) ===')
    say(f'{"endpoint":<43} {"path":<13} {"class":<21} {"blk/s":>8} {"blocks":>12} {"lag":>8} {"hs/ms":>9}')
    for row in sorted_rows(rows):
        if row['phase'] != 'B':
            continue
        hs = '-' if row['handshake_ms'] is None else f'{row["handshake_ms"]:.1f}'
        lag = '-' if row['lag'] is None else str(row['lag'])
        path = 'SOCKS5' if row['network_mode'] == 'NATIVE_SOCKS5' else 'DIRECT'
        blocks = f'{row["gained_blocks"]}/{row["target_blocks"]}'
        say(f'{row["endpoint"]:<43} {path:<13} {row["classification"]:<21} '
            f'{row["blocks_per_second"]:>8.1f} {blocks:>12} {lag:>8} {hs:>9}')


def interactive_setup(args: argparse.Namespace) -> None:
    if not sys.stdin.isatty():
        raise ProbeError('Non-interactive execution requires --network mainnet|testnet.')
    say('XPR Peer Probe — setup')
    choice = input('Network [1=mainnet, 2=testnet] (1): ').strip() or '1'
    if choice not in ('1','2','mainnet','testnet'):
        raise ProbeError('Choose 1/mainnet or 2/testnet.')
    args.network = 'mainnet' if choice in ('1','mainnet') else 'testnet'
    if not args.native_socks5:
        proxy = input('SOCKS5 host:port (empty = DIRECT): ').strip()
        if proxy:
            args.native_socks5 = normalize_endpoint(proxy)
    if not args.snapshot:
        snap = input('Existing .bin snapshot path (empty = network cache/download): ').strip()
        if snap:
            args.snapshot = snap
    if not args.peers_file and not args.peer and not args.peers:
        peers_file = input(f'Peers file (empty = peers/{args.network}.json): ').strip()
        if peers_file:
            args.peers_file = peers_file


def positive_int(value: str) -> int:
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError('must be positive')
    return result


def positive_float(value: str) -> float:
    result = float(value)
    if result <= 0 or not math.isfinite(result):
        raise argparse.ArgumentTypeError('must be finite and positive')
    return result


def port_type(value: str) -> int:
    result = int(value)
    if not 1 <= result <= 65535:
        raise argparse.ArgumentTypeError('must be 1..65535')
    return result


def make_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description='XPR Mainnet/Testnet P2P probe using an isolated nodeos.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog='No production files are edited. No ProxyChains, pip, jq, curl, Redis or Docker required.')
    p.add_argument('--version', action='version', version=VERSION)
    general = p.add_argument_group('Network and peers')
    general.add_argument('--network', choices=NETWORKS, help='Network profile. Omit for interactive setup.')
    general.add_argument('--interactive', action='store_true', help='Show the setup questions even with --network.')
    general.add_argument('--peers-file', help='JSON, host:port text list or p2p-peer-address config lines.')
    general.add_argument('--peer', action='append', default=[], metavar='HOST:PORT', help='Test a single endpoint; repeatable. Overrides peer file.')
    general.add_argument('--peers', action='append', default=[], metavar='A:PORT,B:PORT', help='Comma-separated explicit endpoints; overrides peer file.')
    general.add_argument('--exclude-peer', '--self-peer', dest='exclude_peer', action='append', default=[],
                         help='Exclude an exact normalized endpoint; repeatable. No Saltant-specific default.')
    general.add_argument('--list-peers', action='store_true', help='Validate and display selected peers without starting nodeos.')
    general.add_argument('--workspace', default='.xpr-peerprobe', help='Data/cache/report root, relative to working directory.')
    general.add_argument('--check', action='store_true', help='Check nodeos, options, ports and resources only; no snapshot download.')
    deps = p.add_argument_group('Dependencies')
    deps.add_argument('--nodeos-bin', help='Explicit compatible nodeos executable.')
    deps.add_argument('--install-deps', action='store_true', help='Offer local pinned Leap installation if nodeos is missing; never upgrade existing nodeos.')
    deps.add_argument('--yes', action='store_true', help='Approve dependency installation only together with --install-deps; not resource overrides.')
    deps.add_argument('--nodeos-sha256', help='Optional independently obtained SHA-256 for pinned .deb; signature verification still required.')
    snap = p.add_argument_group('Snapshot')
    snap.add_argument('--snapshot', help='Exact .bin; bypass download. Its chain must match --network.')
    snap.add_argument('--snapshot-sha256', help='Require this SHA-256 before testing.')
    snap.add_argument('--snapshot-url', help='Override profile HTTPS download URL; explicitly downloads this source.')
    snap.add_argument('--snapshot-format', choices=['auto','tar','bin'], default='auto', help='Download format, auto-detected by content.')
    snap.add_argument('--refresh-snapshot', action='store_true', help='Download again; keep old cached snapshots/reports intact.')
    snap.add_argument('--skip-download', action='store_true', help='Require cached or explicit snapshot.')
    snap.add_argument('--keep-archive', action='store_true', help='Retain compressed download as well as .bin; normally only .bin is cached.')
    snap.add_argument('--max-download-gib', type=positive_float, default=10.0, help='Maximum compressed/raw download size.')
    snap.add_argument('--max-snapshot-gib', type=positive_float, default=20.0, help='Maximum extracted snapshot size.')
    proxy = p.add_argument_group('Native SOCKS5 (no LD_PRELOAD)')
    proxy.add_argument('--native-socks5', metavar='HOST:PORT', help='Existing SOCKS5 proxy; native relay is created automatically.')
    proxy.add_argument('--relay-remote-dns', action='store_true', help='Send destination hostname to SOCKS5 instead of a pinned local IP.')
    proxy.add_argument('--ip-family', choices=['4','6'], default='4', help='Local destination DNS address family.')
    proxy.add_argument('--compare', action='store_true', help='DIRECT/SOCKS5 pair per peer, same snapshot and pinned IP; requires proxy, local DNS.')
    proxy.add_argument('--socks-user-env', help='Environment variable containing proxy username; never saved in reports.')
    proxy.add_argument('--socks-pass-env', help='Environment variable containing proxy password; never saved in reports.')
    proxy.add_argument('--proxy-timeout', type=positive_float, default=10.0, help='SOCKS5 connect/auth timeout in seconds.')
    test = p.add_argument_group('Test and timing')
    test.add_argument('--with-phase-a', action='store_true', help='Add isolated handshake/hold samples; does not filter Phase B.')
    test.add_argument('--phase-a-hold', type=positive_float, default=2.0, help='Phase A connection hold in seconds.')
    test.add_argument('--rounds', type=positive_int, default=1, help='Repeated rounds; maximum 10.')
    test.add_argument('--pause', type=positive_float, default=2.0, help='Pause between sample lifecycles in seconds.')
    test.add_argument('--handshake-timeout', type=positive_float, default=10.0, help='Time to observe valid Antelope handshake, excluding restore.')
    test.add_argument('--catchup-blocks', type=positive_int, default=2000, help='Full historical target required for GOOD.')
    test.add_argument('--catchup-timeout', type=positive_float, default=45.0, help='Total catch-up budget from net/connect, including handshake.')
    test.add_argument('--stall-seconds', type=positive_float, default=8.0, help='Stop after this much time with no local HEAD progress.')
    test.add_argument('--poll-interval', type=positive_float, default=0.1, help='Local API observation interval, in seconds; not ping resolution.')
    test.add_argument('--api-timeout', type=positive_float, default=3.0, help='Per-local-RPC timeout in seconds.')
    test.add_argument('--api-start-timeout', type=positive_float, default=300.0, help='Snapshot restore/API readiness timeout in seconds.')
    test.add_argument('--shutdown-timeout', type=positive_float, default=600.0, help='Graceful SIGTERM wait; no automatic SIGKILL.')
    test.add_argument('--dns-timeout', type=positive_float, default=10.0, help='Local destination DNS wait in seconds.')
    limits = p.add_argument_group('Ports and resource guardrails')
    limits.add_argument('--http-port', type=port_type, help='Diagnostic HTTP loopback port; mainnet 18888, testnet 28888.')
    limits.add_argument('--p2p-port', type=port_type, help='Diagnostic P2P loopback port; mainnet 19876, testnet 29876.')
    limits.add_argument('--relay-port', type=port_type, help='Native relay loopback port; mainnet 19000, testnet 29000.')
    limits.add_argument('--chain-db-mb', type=positive_int, default=16384, help='chain-state-db-size-mb; capacity, not measured resident memory.')
    limits.add_argument('--chain-threads', type=positive_int, default=8, help='Diagnostic nodeos chain threads.')
    limits.add_argument('--sync-fetch-span', type=positive_int, default=2000, help='nodeos historical request chunk size.')
    limits.add_argument('--min-free-gib', type=positive_float, default=8.0, help='Minimum available disk before each restore/download reserve.')
    limits.add_argument('--min-ram-gib', type=positive_float, default=4.0, help='Minimum MemAvailable/cgroup headroom; conservative guard, not capacity sizing.')
    limits.add_argument('--allow-low-resources', action='store_true', help='Explicitly bypass RAM/disk minimum checks. Risk of affecting production.')
    limits.add_argument('--keep-temp', action='store_true', help='Keep each sample data-dir; disk usage grows with peers × rounds × modes.')
    limits.add_argument('--recommend', type=positive_int, default=6, help='Number of all-rounds GOOD candidates exported for each path.')
    return p


def validate_args(args: argparse.Namespace) -> None:
    if args.snapshot and (args.refresh_snapshot or args.snapshot_url):
        raise ProbeError('--snapshot cannot be combined with --refresh-snapshot/--snapshot-url.')
    if args.skip_download and (args.refresh_snapshot or args.snapshot_url):
        raise ProbeError('--skip-download conflicts with requesting a new download.')
    if args.compare and not args.native_socks5:
        raise ProbeError('--compare needs --native-socks5 HOST:PORT.')
    if args.compare and args.relay_remote_dns:
        raise ProbeError('--compare pins the same destination IP; omit --relay-remote-dns.')
    if args.rounds > 10:
        raise ProbeError('--rounds maximum is 10 to limit repeated connection attempts.')
    if args.catchup_timeout <= args.handshake_timeout:
        raise ProbeError('--catchup-timeout must exceed --handshake-timeout.')
    if bool(args.socks_user_env) != bool(args.socks_pass_env):
        raise ProbeError('Both --socks-user-env and --socks-pass-env are required for proxy auth.')
    if (args.socks_user_env or args.relay_remote_dns) and not args.native_socks5:
        raise ProbeError('Proxy authentication/remote DNS requires --native-socks5.')
    for key in ('snapshot_sha256','nodeos_sha256'):
        value = getattr(args, key)
        if value and not re.fullmatch('[0-9a-fA-F]{64}', value):
            raise ProbeError(f'--{key.replace("_", "-")} requires 64 hexadecimal characters.')
    for key in ('http_port','p2p_port','relay_port'):
        if getattr(args, key) is None:
            setattr(args, key, NETWORKS[args.network][key])


def execute(args: argparse.Namespace) -> int:
    if platform.system() != 'Linux':
        raise ProbeError('The diagnostic runner currently supports Linux only.')
    if os.environ.get('LD_PRELOAD') or os.environ.get('PROXYCHAINS_CONF_FILE'):
        raise ProbeError('Do not wrap the probe in ProxyChains/LD_PRELOAD. Use --native-socks5.')
    if not args.network or args.interactive:
        interactive_setup(args)
    validate_args(args)
    peers, peer_source = select_peers(args)
    say(f'XPR Peer Probe {VERSION}\nNetwork: {args.network}; {len(peers)} endpoint(s) from {peer_source}')
    if args.list_peers:
        for peer in peers:
            say(peer)
        return 0
    original_root = Path(args.workspace).expanduser().absolute()
    if original_root.is_symlink():
        raise ProbeError('Workspace must not be a symlink.')
    root = original_root.resolve()
    with WorkspaceLock(root):
        check_orphan(root)
        ports = [args.http_port, args.p2p_port] + ([args.relay_port] if args.native_socks5 else [])
        check_ports(ports)
        preflight = check_resources(root, args)
        executable, nodeos_version = ensure_nodeos(root, args)
        say(f'nodeos: {executable}\nVersion: {nodeos_version}')
        proxy = parse_endpoint(args.native_socks5) if args.native_socks5 else None
        credentials = None
        if args.socks_user_env:
            try:
                credentials = (os.environ[args.socks_user_env], os.environ[args.socks_pass_env])
            except KeyError as exc:
                raise ProbeError('A requested SOCKS credential environment variable is not set.') from exc
        if proxy:
            with open_socks(proxy, None, args.proxy_timeout, credentials):
                say('SOCKS5 method/auth preflight passed. This is not a P2P connectivity test.')
        if args.check:
            say('Preflight passed. No snapshot was downloaded and no nodeos was started.')
            return 0
        if os.geteuid() == 0:
            say('WARNING: running as root. Prefer an unprivileged operator account; no privileged ports are needed.')
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        requested = min(65535, hard if hard != resource.RLIM_INFINITY else 65535)
        if soft < requested:
            resource.setrlimit(resource.RLIMIT_NOFILE, (requested, hard))
        snapshot, digest, snapshot_source = prepare_snapshot(args, root)
        run_id = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8]
        run_dir = root / args.network / 'runs' / run_id
        run_dir.mkdir(parents=True)
        (run_dir / 'data').mkdir()
        modes = ['DIRECT', 'NATIVE_SOCKS5'] if args.compare else ['NATIVE_SOCKS5' if proxy else 'DIRECT']
        safe_options = {k:v for k,v in vars(args).items() if k not in ('socks_user_env','socks_pass_env')}
        report = {'script_version': VERSION, 'run_id': run_id, 'state':'RUNNING', 'network':args.network,
                  'started_at':utcnow(), 'nodeos_version':nodeos_version,
                  'nodeos_executable':str(executable), 'snapshot_path':str(snapshot),
                  'snapshot_sha256':digest, 'snapshot_source':snapshot_source,
                  'selected_peers':peers, 'peer_source':peer_source, 'options':safe_options,
                  'proxy_auth_used':bool(credentials), 'proxy_endpoint':args.native_socks5,
                  'network_modes':modes, 'preflight':preflight, 'samples':[]}
        atomic_json(run_dir / 'run.json', {'run_id':run_id, 'script_version':VERSION, 'created_at':utcnow()})
        (run_dir / 'peers.json').write_text(json.dumps(peers, indent=2) + '\n')
        write_reports(run_dir, report, args)
        say(f'Reports for this run: {run_dir / "results"}')
        say('WARNING: isolated files/ports do not isolate CPU, RAM, I/O or bandwidth from a production node.')
        if args.keep_temp:
            say('WARNING: --keep-temp retains EVERY sample database; budget disk accordingly.')
        node = Nodeos(executable, args, run_dir)
        baseline = None
        index = 0
        peak_data = 0
        exit_code = 0
        resolved: dict[str, tuple[list[str], str | None]] = {}
        try:
            # Resolve once per endpoint, pinning exactly the same IP for A/B and repeated rounds.
            for peer in peers:
                host, port = parse_endpoint(peer)
                try:
                    ips = resolve_host(host, port, args.ip_family, args.dns_timeout)
                    resolved[peer] = (ips, None)
                except ProbeError as exc:
                    resolved[peer] = ([], str(exc))
            phases = ['A','B'] if args.with_phase_a else ['B']
            if not args.with_phase_a:
                say('Phase A skipped: Phase B already includes handshake validation.')
            for round_number in range(1, args.rounds+1):
                for peer in peers:
                    host, port = parse_endpoint(peer)
                    ips, dns_error = resolved[peer]
                    order = list(reversed(modes)) if args.compare and round_number % 2 == 0 else modes
                    for phase in phases:
                        for mode in order:
                            index += 1
                            row = new_result(peer, mode, phase, round_number)
                            row['resolved_ips'] = ips
                            row['selected_ip'] = ips[0] if ips else None
                            report['samples'].append(row)
                            say(f'[{index}] Phase {phase} round {round_number}/{args.rounds}: {peer} via {mode}')
                            try:
                                if dns_error and not (mode == 'NATIVE_SOCKS5' and args.relay_remote_dns):
                                    row.update(classification='DNS_ERROR', reason=dns_error, finished_at=utcnow())
                                else:
                                    check_resources(root, args)
                                    # Remote DNS is an explicit exception: actual foreign destination IP is unknown.
                                    use_host = host if mode == 'NATIVE_SOCKS5' and args.relay_remote_dns else ips[0]
                                    identifier = f'{index:04d}-{phase.lower()}-{mode.lower()}'
                                    baseline = run_sample(node, args, run_dir, snapshot, baseline, row,
                                                          (use_host, port), proxy, credentials, identifier)
                                    report['snapshot'] = baseline
                            except KeyboardInterrupt:
                                row.update(classification='INTERRUPTED', reason='Operator interrupted the test.')
                                raise
                            finally:
                                peak_data = max(peak_data, row.get('data_disk_bytes_after_stop', 0))
                                write_reports(run_dir, report, args)
                            say(f'  {row["classification"]}: {row["gained_blocks"]}/{row["target_blocks"]} '
                                f'blocks, {row["blocks_per_second"]:.1f} blk/s; {row["reason"]}')
                            time.sleep(args.pause)
            report['state'] = 'COMPLETE'
        except KeyboardInterrupt:
            report['state'] = 'INTERRUPTED'
            exit_code = 130
        except Exception as exc:
            report.update(state='FAILED', error=str(exc))
            say(f'ERROR: {exc}')
            exit_code = 2
        finally:
            try:
                node.stop()
            except Exception as exc:
                report.update(state='FAILED', shutdown_error=str(exc))
                exit_code = 2
            allocated, apparent = tree_size(run_dir)
            data_total, _ = tree_size(run_dir / 'data')
            network_total, _ = tree_size(root / args.network)
            tools_total, _ = tree_size(root / 'tools')
            snapshot_size, _ = tree_size(snapshot)
            external_size = 0 if snapshot.is_relative_to(root) else snapshot_size
            # Not a filesystem high-water mark: estimate using stopped data sizes; refresh overlap not included.
            permanent = max(0, network_total - data_total)
            estimate = permanent + tools_total + external_size + (data_total if args.keep_temp else peak_data)
            report['storage'] = {'run_disk_bytes':allocated, 'run_apparent_bytes':apparent,
                'peak_single_stopped_data_bytes':peak_data, 'retained_data_bytes':data_total,
                'network_persistent_bytes':permanent, 'local_tools_bytes':tools_total,
                'external_snapshot_bytes':external_size, 'estimated_total_footprint_bytes':estimate,
                'filesystem_free_after_bytes':shutil.disk_usage(root).free,
                'measurement_note':'Allocated filesystem blocks, after graceful shutdown. Estimate, not sampled peak; '
                                   'CoW/compression and snapshot-refresh overlap may differ.'}
            report['finished_at'] = utcnow()
            write_reports(run_dir, report, args)
        print_ranking(report['samples'])
        say(f'\nRun state: {report["state"]}\nResults: {run_dir / "results"}\n'
            f'Largest stopped temporary node: {human_bytes(peak_data)}')
        if exit_code == 0:
            candidates = [r for r in report['samples'] if r['phase']=='B' and r['classification']=='GOOD']
            if not candidates:
                say('No endpoint completed the requested target. See classification reasons; this is not a claim all peers are down.')
                exit_code = 3
        return exit_code


def main(argv: list[str] | None = None) -> int:
    parser = make_parser()
    args = parser.parse_args(argv)
    interrupted = False
    def on_signal(signum, frame):
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            raise KeyboardInterrupt
        say('Shutdown already requested; waiting for the diagnostic nodeos to stop safely.')
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, on_signal)
    try:
        return execute(args)
    except KeyboardInterrupt:
        say('Interrupted before testing. Existing reports were not deleted.')
        return 130
    except (ProbeError, OSError, urllib.error.URLError, tarfile.TarError) as exc:
        say(f'ERROR: {exc}')
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
