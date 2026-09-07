# XPR Peer Probe

**Test whether an XPR Network P2P endpoint can actually synchronize another node — not just accept a TCP connection.**

XPR Peer Probe starts an isolated diagnostic `nodeos`, restores a snapshot, checks the Antelope handshake and chain ID, and measures a real historical-block catch-up. It supports **Mainnet**, **Testnet**, direct connections, and a built-in **TCP → SOCKS5 relay**.

Each endpoint starts from the **same snapshot within a run**. Production configuration, keys, services and data directories are not edited.

> **Release candidate: `2.0.0-rc1`.** The earlier v1.5 workflow was exercised by operators with Leap 5.0.3. This refactored version has automated local tests, including a mock nodeos and SOCKS5 server; it still needs a real-node smoke test before being tagged as a stable release.

## Quick start

Download or clone this repository, then run commands from its directory.

### An existing nodeos is installed

```bash
python3 xpr_peer_probe.py --network mainnet --check
python3 xpr_peer_probe.py --network mainnet
```

### A fresh Ubuntu host

```bash
chmod +x setup.sh xpr_peer_probe.py
./setup.sh --network mainnet
./xpr_peer_probe.py --network mainnet
```

`setup.sh` checks Python and CA certificates, then runs the dependency checker. Missing installation helpers and a private Leap installation are offered **with confirmation**. It does not start a production service, modify `/usr/bin/nodeos`, or upgrade an existing nodeos.

Run without arguments for a small interactive setup:

```bash
./xpr_peer_probe.py
```

### Switch networks

```bash
./xpr_peer_probe.py --network mainnet
./xpr_peer_probe.py --network testnet
```

The network selects its own **chain ID, snapshot source, peer file, ports, cache and reports**. A Mainnet snapshot supplied with `--network testnet` stops the run as a **local configuration error**, before testing any peer.

## Requirements

| Component | Requirement |
|---|---|
| Operating system | Linux. Automated Leap installation is restricted to **Ubuntu 22.04/24.04, amd64**. Other Linux systems require a compatible manually installed nodeos. |
| Python | **3.10+**, standard library only. No `pip install` step. |
| Blockchain binary | A compatible Antelope **Leap `nodeos`**. The reference/pinned installer version is **5.0.3**, not an arbitrary latest release. `--nodeos-bin` selects another executable. |
| Network | Outbound P2P TCP; HTTPS for automatic downloads. An existing reachable SOCKS5 service is required only for relay mode. |
| Disk | Default guard: **8 GiB free** before a restore and as a reserve during snapshot downloads. Allocate more for retained snapshots, repeated runs or `--keep-temp`. |
| Memory | Default guard: **4 GiB available**, not merely installed. This is a preliminary safety check, **not a guarantee that any snapshot will fit**. Leave substantial additional headroom on production hosts. |
| Privileges | Normal probing uses an unprivileged account. Only explicitly approved system-package installation may need `sudo`. |

`curl`, `wget`, `jq`, `cleos`, `proxychains`, Docker, Hyperion, Redis and RabbitMQ are **not dependencies** of the probe. `dpkg-deb` and GnuPG are needed only for the optional private Leap installation.

**Do not confuse a short snapshot-based probe with a full-history node.** It does not download the entire blockchain. Actual storage depends on the snapshot, chain database, filesystem and selected test size. Stopped-node disk usage is recorded in `storage.txt`; sparse file allocation and apparent size are kept separate.

### Dependency installation and trust

The optional installer downloads the official pinned `.deb`, verifies its detached OpenPGP signature in a **temporary keyring**, and uses `dpkg-deb -x` to extract it under `.xpr-peerprobe/tools/`. Package maintainer scripts are not executed. The existing system binary takes precedence unless `--nodeos-bin` is supplied.

Maintainer keys are retrieved over HTTPS from the GitHub key URLs listed by the [XPR installation guide](https://github.com/XPRNetwork/xpr.start). This trusts those HTTPS/GitHub sources; it is **not an independently pinned trust root**. An independently obtained package checksum can additionally be supplied with `--nodeos-sha256`.

Missing runtime libraries may be offered through APT after a dependency-plan preview. Plans containing removals/upgrades are refused for manual review. Treat any approved APT transaction as a system change, especially on a producer host. An incompatible OS, CPU or binary is reported rather than blindly upgraded.

For unattended installation, both flags are needed:

```bash
./setup.sh --network mainnet --install-deps --yes
```

`--yes` alone does not authorize installing nodeos or bypass resource checks.

## Peer lists

Default lists are regular editable files:

```text
peers/mainnet.json
peers/testnet.json
```

They are loaded relative to the script directory; the workspace is relative to the **current working directory**, unless `--workspace` is supplied. Seed lists are candidates, **not promises of current availability**. See [peer-list provenance](peers/README.md).

The original JSON format is supported; extra metadata such as `location` or `node_type` is ignored:

```json
[
  {"type": "p2p", "status": "active", "url": "mainnet.brotonbp.com:9876"},
  {"type": "p2p", "status": "active", "url": "proton.protonuk.io:9876"}
]
```

String arrays, one `host:port` per line, and `p2p-peer-address = host:port` lines also work. In object entries, explicitly non-P2P or non-active records are skipped. Duplicates are removed. Accidental `http://` or `https://` prefixes are removed **with a visible notice**; P2P traffic remains raw TCP, not HTTP. Paths and embedded credentials are rejected.

Use a custom file or specific endpoints:

```bash
./xpr_peer_probe.py --network mainnet --peers-file ./my-mainnet-peers.json

./xpr_peer_probe.py --network mainnet --peer mainnet.brotonbp.com:9876

./xpr_peer_probe.py --network mainnet \
  --peers mainnet.brotonbp.com:9876,proton.protonuk.io:9876
```

`--peer` / `--peers` override the file. Repeat `--exclude-peer HOST:PORT` to skip your own endpoint or any other candidate. `--self-peer` remains an alias; there is **no operator-specific automatic exclusion**.

Preview without nodeos or network testing:

```bash
./xpr_peer_probe.py --network testnet --list-peers
```

## Snapshots and reproducibility

A missing snapshot is downloaded from the selected network's EOSUSA source. Both `.tar.gz` archives and raw `.bin` downloads are supported. Snapshots are cached by SHA-256 under the corresponding network; old snapshots are not automatically deleted.

```bash
# Fetch the source again. "Latest" may still be the same snapshot.
./xpr_peer_probe.py --network mainnet --refresh-snapshot

# Use an exact local snapshot; do not download another one.
./xpr_peer_probe.py --network testnet --snapshot /path/to/testnet-snapshot.bin
```

`--snapshot-sha256 HASH` verifies a known digest. A checksum proves file equality, not the trustworthiness of its provider. Use trusted snapshot sources. Archives with unsafe paths, links, devices, multiple `.bin` files or oversized contents are rejected.

For comparison between hosts or routes, keep the **snapshot SHA-256, target block count, nodeos version and destination IP** consistent. Tests taken at different times may still encounter different remote load or policies.

## SOCKS5 relay and DIRECT / relay comparison

No separate relay process or ProxyChains installation is required:

```bash
./xpr_peer_probe.py --network mainnet \
  --snapshot /path/to/mainnet-snapshot.bin \
  --native-socks5 127.0.0.1:10809 \
  --peers mainnet.brotonbp.com:9876,protonp2p.blocksindia.com:9876
```

```text
isolated nodeos → loopback TCP relay → SOCKS5 → configured egress → P2P peer
```

The probe **does not install or configure Xray**. A SOCKS5 address alone does not prove a foreign exit: verify the proxy's routing separately. Proxy credentials can be supplied through environment-variable names; see [CLI reference](docs/CLI.md).

Run both paths automatically:

```bash
./xpr_peer_probe.py --network mainnet \
  --snapshot /path/to/mainnet-snapshot.bin \
  --native-socks5 127.0.0.1:10809 --compare \
  --peers mainnet.brotonbp.com:9876,protonp2p.blocksindia.com:9876
```

Comparison mode pins the same locally resolved destination IP for both paths. `--rounds 2` repeats measurements and reverses path order on the second round. Remote DNS is intentionally incompatible with this pinned-IP comparison. A SOCKS run changes both the network path **and the source IP seen by the peer**; source-IP policy remains an alternative explanation to filtering.

SOCKS5 applies to diagnostic P2P only. Snapshot/package downloads do not use this relay. Relay counters are application payload, not a hosting-provider bandwidth invoice.

## Reading the results

| Result | Meaning |
|---|---|
| `GOOD` | Correct-network handshake and the **entire requested target** observed within the configured catch-up budget. This is not a long-term uptime guarantee. |
| `SLOW` | Valid handshake, but target was not completed in time or local HEAD stopped progressing. Read `reason` and the log. |
| `PEER_CLOSED` | The P2P connection was rejected/closed. Slots, duplicate connections, source-IP policy and overload are possible; this does **not** prove the node is offline. |
| `SILENT` | No valid Antelope handshake before the deadline. This label alone does **not** prove TCP established. |
| `WRONG_CHAIN` | Received chain ID mismatch or a remote `go_away` wrong-chain reason. Evidence distinguishes the two. |
| `CONNECT_FAIL` / `DNS_ERROR` | Transport connection or local name resolution failed. |
| `PROXY_ERROR` | Native relay/SOCKS setup or forwarding failed. Not an endpoint-health verdict. |
| `NO_HISTORY` | Peer's advertised head is not ahead of the snapshot. |
| `INSUFFICIENT_HISTORY` | Advertised LIB does not leave enough irreversible blocks ahead for the full target. Use an older snapshot or intentionally smaller target. This does not prove retention depth. |
| `LOCAL_ERROR` / `INTERRUPTED` | Local runner/configuration/runtime problem or operator interruption. Never classify these as dead remote peers. |
| `HANDSHAKE_OK` | Optional Phase A only: handshake and short hold succeeded, without a throughput verdict. |

**`lag`** is `remote_head − snapshot_head` at handshake, in blocks. It is **not ping**, not the peer's lag behind the live network, and should not be compared across sequential tests as a freshness ranking.

**`hs`** is time from `net/connect` to observing a valid handshake through the local API. It includes connection/proxy setup and polling overhead; it is not pure RTT.

**`blk/s`** is observed local HEAD advancement divided by elapsed time from `net/connect`, excluding snapshot restoration/shutdown. It includes local validation/application cost. Slightly more than `2000/2000` is expected because sampling is periodic. A higher score is not necessarily a faster remote server.

Phase A is optional (`--with-phase-a`) and does not filter Phase B. The default Phase B already validates a real handshake and tests catch-up. Phase A may itself receive blocks while holding the connection; it is not a zero-traffic probe.

## Reports and safety

```text
.xpr-peerprobe/
├── tools/                         # Optional private Leap binary
├── mainnet/
│   ├── snapshots/                 # Content-addressed .bin files and current.json
│   ├── latest.json                # Pointer to the latest run and its state
│   └── runs/<UTC-time>-<id>/
│       ├── logs/                  # Per-sample nodeos logs
│       ├── configs/               # Generated diagnostic configs
│       ├── data/                  # Temporary; removed after shutdown by default
│       ├── peers.json             # Selected candidates for this run
│       └── results/
│           ├── results.json       # Full metadata, observations, evidence, counters
│           ├── phase-a.csv
│           ├── phase-b.csv
│           ├── report.md
│           ├── p2p-peers.conf      # DIRECT candidates only
│           ├── proxy-candidates.txt
│           └── storage.txt
└── testnet/                       # Separate cache and run history
```

Reports are checkpointed after every sample. Previous runs and legacy v1.x directories are **not erased**. The workspace lock prevents concurrent runs in the same workspace. The runner verifies that the local HTTP listener belongs to its own child before sending control RPCs.

Only all-rounds `GOOD` direct candidates enter `p2p-peers.conf`. Proxy-only successes are exported separately: **do not paste a proxy-tested remote address into production and assume direct access works**. No production configuration is edited automatically.

Press **Ctrl+C once** for graceful shutdown. The probe sends SIGTERM only to its own process group. On a shutdown timeout, it preserves the database and PID metadata rather than sending SIGKILL. Inspect the reported PID/log before another run.

**Isolation is not resource isolation.** Snapshot restoration, validation, shutdown writes and downloads still compete with production CPU/RAM/disk/network. Do not run a large benchmark on a busy producer without headroom. Do not expose the diagnostic `net_api_plugin` publicly. Avoid repeated rapid retries against endpoints rejecting connections.

Reports contain endpoint addresses, proxy address and local filesystem paths, but not SOCKS credential values. Review metadata and logs before publishing them.

## Useful options

| Option | Purpose |
|---|---|
| `--network mainnet` / `--network testnet` | Select profile and isolated network workspace. |
| `--check`, `--list-peers` | Preflight or preview without a heavy test. |
| `--peers-file PATH` | Use your own candidate list. |
| `--peer`, `--peers`, `--exclude-peer` | Select or exclude endpoints. |
| `--snapshot PATH`, `--refresh-snapshot` | Reuse an exact snapshot or fetch again. |
| `--native-socks5 HOST:PORT`, `--compare` | Native relay or paired path comparison. |
| `--catchup-blocks 2000`, `--catchup-timeout 45` | Full target and timing budget. |
| `--rounds 2`, `--pause 2` | Repeat tests without rapid reconnect loops. |
| `--nodeos-bin PATH`, `--install-deps` | Select binary or offer private installation. |
| `--workspace PATH` | Put cache and reports on a disk of your choice. |
| `--http-port`, `--p2p-port`, `--relay-port` | Override occupied diagnostic ports. |

All options, defaults, exit codes and migration details are in [docs/CLI.md](docs/CLI.md). Release-candidate testing notes and limitations are summarized in this README.

## Development

```bash
python3 -m unittest discover -s tests -v
bash -n setup.sh
```

The test suite uses loopback-only mock servers; it does not contact public peers or install packages. `tests/fake_nodeos.py` is a test double, **not an Antelope implementation**.

## References

- [XPR Mainnet operator guide](https://github.com/XPRNetwork/xpr.start)
- [XPR Testnet operator guide](https://github.com/XPRNetwork/xpr-testnet.start)
- [Antelope Leap 5.0.3 release](https://github.com/AntelopeIO/leap/releases/tag/v5.0.3)
- [Leap 5.0.3 net plugin connection-status fields](https://github.com/AntelopeIO/leap/blob/v5.0.3/plugins/net_plugin/include/eosio/net_plugin/net_plugin.hpp)
- [SOCKS5: RFC 1928](https://www.rfc-editor.org/rfc/rfc1928)
- [SOCKS username/password authentication: RFC 1929](https://www.rfc-editor.org/rfc/rfc1929)
