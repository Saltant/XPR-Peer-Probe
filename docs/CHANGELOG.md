# Changelog

## 2.0.0-rc1

Public-release refactor of the attached `1.5-native-socks-relay` operator script.

### Added

- Explicit Mainnet/Testnet profiles, independent port defaults, peer files, snapshot caches and run histories.
- Independent expected-chain verification before peer testing; wrong local snapshot is not a peer failure.
- Interactive setup, `--check`, `--list-peers`, `--peers-file`, repeated exclusions and an explicit nodeos path.
- Opt-in pinned Leap installer using a temporary verification keyring and private package extraction.
- A bootstrap shell script for Python/CA certificates; runtime dependency-plan preview.
- Automatic DIRECT/native-SOCKS pair comparison using one locally selected IP per endpoint.
- Repeated rounds and all-rounds-pass recommendations.
- Proxy username/password via environment variables, IPv6 address parsing and native ATYP IPv6 support.
- Bounded relay buffers, backpressure, half-close handling and one upstream connection per sample.
- Per-run reports, partial checkpoints, separate DIRECT/proxy candidate exports, a Markdown report.
- Workspace locking, own-child HTTP listener validation, orphan detection and graceful exception cleanup.
- Archive size/path/type validation and content-addressed snapshot caches.
- Local test suite for CLI, fake nodeos lifecycle, native SOCKS, failure classification and filesystem safety.

### Fixed / clarified

- Attached code mixed a Testnet snapshot URL with Mainnet candidates/default self-exclusion.
- Earlier cleanup deleted logs/results before dependency or port validation. Old runs are now retained.
- Closed sockets may retain a last handshake: `is_socket_open` is checked where exposed.
- RPC/proxy errors are no longer hidden as remote `SILENT` outcomes.
- `GOOD` requires the entire configured target; a tiny available sample is not silently promoted.
- Target availability is based on advertised irreversible progress, not only live head.
- Timing numerator and denominator use one observation; late observations do not count as on-time GOOD.
- Storage is measured after shutdown; reported total remains an estimate, not a guaranteed high-water mark.
- `lag` is not remote node freshness; `hs` is not raw network RTT; a good endpoint is not necessarily caught up.
- Slot exhaustion/source-IP policy cannot be proven from a generic close. Reports no longer imply a specific cause.

### Removed / changed

- Legacy ProxyChains `--socks5`, `--proxychains-bin`, `--proxy-local-dns` modes are removed, not silently remapped.
- A single implicit `./peers.json` is no longer used. Use `peers/<network>.json` or explicitly select the legacy file.
- No hard-coded operator-specific proxy, public IP, or self-peer exclusion.
- `--min-confidence-blocks` is replaced by an explicit full-target requirement.
- `--delete-archive` is replaced by default non-retention of new compressed archives and opt-in `--keep-archive`.
- `--keep-temp` retains each sample's data, not only the last sample. The disk warning is explicit.
- Phase A can run through the native relay; it remains off by default.
