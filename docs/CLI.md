# CLI reference

Applies to `2.0.0-rc1`. Generated option list is based on the actual argument parser.

Run `python3 xpr_peer_probe.py --help` for terminal help. Options are grouped below; most users only need `--network`, an optional `--snapshot`, and an optional `--native-socks5`.

## Options

| Option | Default | Purpose |
|---|---|---|
| `-h`, `--help` | `—` | show this help message and exit |
| `--version` | `—` | show program's version number and exit |

## Network And Peers

| Option | Default | Purpose |
|---|---|---|
| `--network` | `—` | Network profile. Omit for interactive setup. |
| `--interactive` | `False` | Show the setup questions even with --network. |
| `--peers-file` | `—` | JSON, host:port text list or p2p-peer-address config lines. |
| `--peer` | `[]` | Test a single endpoint; repeatable. Overrides peer file. |
| `--peers` | `[]` | Comma-separated explicit endpoints; overrides peer file. |
| `--exclude-peer`, `--self-peer` | `[]` | Exclude an exact normalized endpoint; repeatable. No Saltant-specific default. |
| `--list-peers` | `False` | Validate and display selected peers without starting nodeos. |
| `--workspace` | `.xpr-peerprobe` | Data/cache/report root, relative to working directory. |
| `--check` | `False` | Check nodeos, options, ports and resources only; no snapshot download. |

## Dependencies

| Option | Default | Purpose |
|---|---|---|
| `--nodeos-bin` | `—` | Explicit compatible nodeos executable. |
| `--install-deps` | `False` | Offer local pinned Leap installation if nodeos is missing; never upgrade existing nodeos. |
| `--yes` | `False` | Approve dependency installation only together with --install-deps; not resource overrides. |
| `--nodeos-sha256` | `—` | Optional independently obtained SHA-256 for pinned .deb; signature verification still required. |

## Snapshot

| Option | Default | Purpose |
|---|---|---|
| `--snapshot` | `—` | Exact .bin; bypass download. Its chain must match --network. |
| `--snapshot-sha256` | `—` | Require this SHA-256 before testing. |
| `--snapshot-url` | `—` | Override profile HTTPS download URL; explicitly downloads this source. |
| `--snapshot-format` | `auto` | Download format, auto-detected by content. |
| `--refresh-snapshot` | `False` | Download again; keep old cached snapshots/reports intact. |
| `--skip-download` | `False` | Require cached or explicit snapshot. |
| `--keep-archive` | `False` | Retain compressed download as well as .bin; normally only .bin is cached. |
| `--max-download-gib` | `10.0` | Maximum compressed/raw download size. |
| `--max-snapshot-gib` | `20.0` | Maximum extracted snapshot size. |

## Native Socks5 (No Ld_Preload)

| Option | Default | Purpose |
|---|---|---|
| `--native-socks5` | `—` | Existing SOCKS5 proxy; native relay is created automatically. |
| `--relay-remote-dns` | `False` | Send destination hostname to SOCKS5 instead of a pinned local IP. |
| `--ip-family` | `4` | Local destination DNS address family. |
| `--compare` | `False` | DIRECT/SOCKS5 pair per peer, same snapshot and pinned IP; requires proxy, local DNS. |
| `--socks-user-env` | `—` | Environment variable containing proxy username; never saved in reports. |
| `--socks-pass-env` | `—` | Environment variable containing proxy password; never saved in reports. |
| `--proxy-timeout` | `10.0` | SOCKS5 connect/auth timeout in seconds. |

## Test And Timing

| Option | Default | Purpose |
|---|---|---|
| `--with-phase-a` | `False` | Add isolated handshake/hold samples; does not filter Phase B. |
| `--phase-a-hold` | `2.0` | Phase A connection hold in seconds. |
| `--rounds` | `1` | Repeated rounds; maximum 10. |
| `--pause` | `2.0` | Pause between sample lifecycles in seconds. |
| `--handshake-timeout` | `10.0` | Time to observe valid Antelope handshake, excluding restore. |
| `--catchup-blocks` | `2000` | Full historical target required for GOOD. |
| `--catchup-timeout` | `45.0` | Total catch-up budget from net/connect, including handshake. |
| `--stall-seconds` | `8.0` | Stop after this much time with no local HEAD progress. |
| `--poll-interval` | `0.1` | Local API observation interval, in seconds; not ping resolution. |
| `--api-timeout` | `3.0` | Per-local-RPC timeout in seconds. |
| `--api-start-timeout` | `300.0` | Snapshot restore/API readiness timeout in seconds. |
| `--shutdown-timeout` | `600.0` | Graceful SIGTERM wait; no automatic SIGKILL. |
| `--dns-timeout` | `10.0` | Local destination DNS wait in seconds. |

## Ports And Resource Guardrails

| Option | Default | Purpose |
|---|---|---|
| `--http-port` | `—` | Diagnostic HTTP loopback port; mainnet 18888, testnet 28888. |
| `--p2p-port` | `—` | Diagnostic P2P loopback port; mainnet 19876, testnet 29876. |
| `--relay-port` | `—` | Native relay loopback port; mainnet 19000, testnet 29000. |
| `--chain-db-mb` | `16384` | chain-state-db-size-mb; capacity, not measured resident memory. |
| `--chain-threads` | `8` | Diagnostic nodeos chain threads. |
| `--sync-fetch-span` | `2000` | nodeos historical request chunk size. |
| `--min-free-gib` | `8.0` | Minimum available disk before each restore/download reserve. |
| `--min-ram-gib` | `4.0` | Minimum MemAvailable/cgroup headroom; conservative guard, not capacity sizing. |
| `--allow-low-resources` | `False` | Explicitly bypass RAM/disk minimum checks. Risk of affecting production. |
| `--keep-temp` | `False` | Keep each sample data-dir; disk usage grows with peers × rounds × modes. |
| `--recommend` | `6` | Number of all-rounds GOOD candidates exported for each path. |

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Preflight/list operation succeeded, or test completed with at least one Phase B GOOD sample. Inspect the report for the other endpoints. |
| `2` | Local setup, argument, dependency, snapshot, process or runtime failure. |
| `3` | All requested tests completed but no Phase B sample was GOOD. This is **not** a claim that every endpoint is offline. |
| `130` | Interrupted by the operator/SIGTERM; graceful cleanup is attempted. |

A rate is not a reliability score. With repeated rounds, the candidate export requires GOOD in **every requested round**, even if the process exit code is zero due to another GOOD sample.

## Common recipes

### Preserve an old operator peer file

```bash
./xpr_peer_probe.py --network mainnet \
  --peers-file ~/xpr-peer-test/peers.json \
  --exclude-peer p2p-protonmain.saltant.io:9876
```

### Existing Testnet snapshot and custom file

```bash
./xpr_peer_probe.py --network testnet \
  --snapshot ~/xpr-peer-testnet/snapshots/latest-snapshot.bin \
  --peers-file ~/xpr-peer-testnet/peers.json \
  --exclude-peer p2p-protontest.saltant.io:9879
```

### Native SOCKS5 with authentication

Set credential environment variables securely in your shell or secret manager, then reference their **names**, not their values:

```bash
./xpr_peer_probe.py --network mainnet \
  --native-socks5 127.0.0.1:10809 \
  --socks-user-env PROBE_SOCKS_USER \
  --socks-pass-env PROBE_SOCKS_PASSWORD
```

The credential values are not written into the report. SOCKS5 username/password authentication itself is not encryption; use a trusted local endpoint or a separately encrypted transport.

### Repeat a DIRECT/native relay comparison

```bash
./xpr_peer_probe.py --network mainnet \
  --snapshot /path/to/mainnet-snapshot.bin \
  --native-socks5 127.0.0.1:10809 --compare --rounds 2 \
  --peers mainnet.brotonbp.com:9876,protonp2p.blocksindia.com:9876
```

The modes are DIRECT→SOCKS5 in round 1, SOCKS5→DIRECT in round 2. Local DNS is resolved once for the run, and the first address in the requested family is used in both paths. Other addresses of a multi-IP hostname are not independently benchmarked. The native mode's `connected_ip` is the local relay address; `selected_ip` records the intended remote target.

When `--relay-remote-dns` is used, the proxy chooses the actual destination address. `resolved_ips` is only the local resolver's observation; it is **not proof** of the remote resolver's selection.

### Find the most recent report without jq

```bash
RESULTS=$(python3 -c 'import json; print(json.load(open(".xpr-peerprobe/mainnet/latest.json"))["results_dir"])')
cat "$RESULTS/report.md"
cat "$RESULTS/p2p-peers.conf"
```

Change `mainnet` to `testnet` for Testnet results. Always check the run state: the pointer may refer to an interrupted/failed run with partial results.

### Export the latest run for review

```bash
RUN=$(python3 -c 'import json; print(json.load(open(".xpr-peerprobe/mainnet/latest.json"))["run_dir"])')
tar -czf peerprobe-report.tar.gz -C "$RUN" results logs configs peers.json run.json
```

This excludes snapshots and chain databases. Review metadata before sharing; it contains addresses and local paths.

## Migration from the supplied v1.5 operator script

Use a new checkout; do not replace your working production relay. This is the **tester**, not the emergency `xpr-p2p-relay` systemd service.

1. Add `--network mainnet` or `--network testnet` to non-interactive commands.
2. Put candidates in `peers/mainnet.json` / `peers/testnet.json`, or add `--peers-file PATH` for an existing `peers.json`.
3. Old snapshot files remain usable through `--snapshot PATH`. Legacy cache directories are not modified or automatically imported.
4. Continue using `--native-socks5`. Legacy `--socks5`, `--proxychains-bin` and `--proxy-local-dns` are deliberately unsupported and fail argument parsing rather than silently changing behavior.
5. `--self-peer` is retained as an alias for repeatable `--exclude-peer`; no default Saltant exclusion remains.
6. Output moved to `<workspace>/<network>/runs/<run-id>/results/`. Use `<workspace>/<network>/latest.json` to find it.
7. Small-sample `GOOD` results are no longer allowed. An insufficient advertised LIB produces `INSUFFICIENT_HISTORY`; reduce the explicit target only deliberately.
8. `--keep-temp` now retains each sample's data. It can consume large amounts of disk when running both modes or multiple rounds.

## Operational boundaries

- The full block target must be available below the peer's advertised LIB at handshake. The tool does not prove how far back the peer retains history.
- The catch-up timer starts before `/v1/net/connect`. Local API calls have their own finite timeout; a final polling observation can overrun the nominal deadline slightly, but late completion is not promoted to GOOD.
- An RPC failure is a local failure, not `SILENT`. Wrong local snapshot network aborts before connecting to peers.
- A generic close cannot reveal `max-clients` or per-host limits unless the remote explicitly supplies evidence. An open TCP port is not sufficient evidence of a usable P2P endpoint.
- `--check` validates the local SOCKS method/auth exchange, not the proxy's final destination or egress. It does not load/validate snapshot contents with nodeos.
- Downloader uses standard HTTPS and the environment's HTTP proxy settings if present. The P2P native relay does not carry package/snapshot downloads.
- There is no unlimited automatic retry, no background daemon/service, and no production restart.
- Some local CPU/I/O and memory costs are unavoidable. A stopped-node allocation estimate is not a measured RAM peak or billing estimate.
- Current version serializes tests within a workspace. Separate workspaces with distinct ports may run simultaneously, but still compete for the host's resources.
