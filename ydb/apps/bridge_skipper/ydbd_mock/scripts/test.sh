#!/usr/bin/env bash
# Run the ydbd_mock unit tests with the virtualenv (the fast loop, and the
# fallback where `ya make -tA` cannot run, e.g. macOS).
#
#   test.sh                        all tests in tests/
#   test.sh test_validator         one module
#   test.sh test_validator.ValidatorTest.test_failover   one test
#   test.sh -e2e [module]          the manual e2e tests in e2e/ (needs a cluster and the CLI)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MOCK_DIR="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$HERE/../../../../.." && pwd)"
WORK="${YDBD_MOCK_WORK:-$HOME/ydbd_mock_work}"

export YDBD_MOCK_WORK="$WORK"
export YDBD_MOCK_RUN_DIR="${YDBD_MOCK_RUN_DIR:-$WORK/run}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$REPO:$WORK/gen${PYTHONPATH:+:$PYTHONPATH}"

DIR="tests"
if [ "${1:-}" = "-e2e" ]; then DIR="e2e"; shift; fi

cd "$MOCK_DIR/$DIR"
if [ $# -eq 0 ]; then
    exec "$WORK/venv/bin/python" -m unittest discover -v -s . -t . -p 'test_*.py'
fi
exec "$WORK/venv/bin/python" -m unittest -v "$@"
