#!/usr/bin/env bash
# Create the virtualenv used to run the mock and the demo outside ya, and
# generate the proto modules. Idempotent.
#
#   setup_venv.sh
#
# Everything goes under $YDBD_MOCK_WORK (default ~/ydbd_mock_work):
#   venv/   virtualenv (do NOT install the `ydb` SDK here, it shadows the generated ydb.* modules)
#   gen/    generated *_pb2.py modules
#   run/    cluster run directory (topology, pids, logs, replicas)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${YDBD_MOCK_WORK:-$HOME/ydbd_mock_work}"
BASE_PY="${YDBD_MOCK_BASE_PYTHON:-python3}"

mkdir -p "$WORK/run/logs" "$WORK/run/state" "$WORK/run/pids"

if [ ! -x "$WORK/venv/bin/python" ]; then
    "$BASE_PY" -m venv "$WORK/venv"
fi

"$WORK/venv/bin/pip" install --quiet --upgrade pip
"$WORK/venv/bin/pip" install --quiet grpcio grpcio-tools protobuf pyyaml aiohttp requests textual

"$HERE/gen_protos.sh"

"$WORK/venv/bin/python" - <<'EOF'
import grpc, google.protobuf, yaml, aiohttp, requests, textual
print("grpc", grpc.__version__, "protobuf", google.protobuf.__version__, "aiohttp", aiohttp.__version__)
EOF
