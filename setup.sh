#!/usr/bin/env bash
# Bootstrap Python/CA certificates on Ubuntu, then run the dependency doctor.
# nodeos is installed privately by Python only when missing and approved.
set -euo pipefail
HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
YES=0
for arg in "$@"; do [[ "$arg" == '--yes' ]] && YES=1; done
MISSING=()
if ! command -v python3 >/dev/null 2>&1; then
  MISSING+=(python3)
elif ! python3 -c 'import sys; sys.exit(sys.version_info < (3,10))'; then
  echo 'Python 3.10+ is needed. Upgrade to a supported Ubuntu or install a supported Python.' >&2
  exit 2
fi
[[ -s /etc/ssl/certs/ca-certificates.crt ]] || MISSING+=(ca-certificates)
if ((${#MISSING[@]})); then
  command -v apt-get >/dev/null || { echo 'Install Python 3.10+ and CA certificates manually.' >&2; exit 2; }
  PREFIX=()
  if ((EUID != 0)); then
    command -v sudo >/dev/null || { echo 'sudo missing: ask an administrator to install dependencies.' >&2; exit 2; }
    PREFIX=(sudo)
  fi
  echo "Missing bootstrap packages: ${MISSING[*]}"
  echo 'The next commands update the apt index and install only these packages.'
  if ((YES == 0)); then
    [[ -t 0 ]] || { echo 'No terminal: use --yes to approve bootstrap installation.' >&2; exit 2; }
    read -r -p 'Continue? [y/N] ' answer
    [[ "$answer" == y || "$answer" == Y || "$answer" == yes ]] || exit 2
  fi
  "${PREFIX[@]}" apt-get update
  "${PREFIX[@]}" apt-get install --no-install-recommends -y "${MISSING[@]}"
fi
# Explicit --network may override this default; argparse uses the last occurrence.
exec python3 "$HERE/xpr_peer_probe.py" --network mainnet --check --install-deps "$@"
