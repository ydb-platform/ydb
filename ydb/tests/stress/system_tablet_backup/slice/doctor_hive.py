#!/usr/bin/env python3
"""Compatibility entry point; CHECKER's parent must be on PYTHONPATH."""
from consistency.hive_recovery import main

if __name__ == "__main__":
    raise SystemExit(main())
