"""`python -m ydb.apps.bridge_skipper.ydbd_mock` entry point (venv runs; not part of the ya build)."""
import sys

from ydb.apps.bridge_skipper.ydbd_mock.main import main

if __name__ == "__main__":
    sys.exit(main())
