#!/usr/bin/env bash
set -euo pipefail

here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(cd -- "$here/../../../.." && pwd)
"$root/ya" make --build relwithdebinfo "$here" >&2
exec "$here/topic_write_sessions_load" "$@"
