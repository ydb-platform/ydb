"""Stress tests for race-prone code paths in the inline-analyze pipeline.

The inline-analyze shim runs inside every ya compile worker, so multiple
processes will routinely:

- write the same per-file cache entry concurrently (same content + same
  cache key, but the writes themselves must not crash with ENOENT),
- synthesize the same probe ``.cpp`` concurrently.

The regression target is the user-reported ``FileNotFoundError`` on
``.json.tmp -> .json`` rename, which surfaced when two parallel
compiles raced on the same temp file path.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Tuple


HERE = Path(__file__).resolve().parent
_REPO = HERE.parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from ydb.tools.include_sanitizer.common import REPO_ROOT  # noqa: E402


def _store_once(args: Tuple[str, str, str]) -> str:
    """Worker: write the same cache entry from a separate process."""
    if args[2] and args[2] not in sys.path:
        sys.path.insert(0, args[2])
    from ydb.tools.include_sanitizer.analyze.cache import store
    target = Path(args[0])
    payload = json.loads(args[1])
    store(target, payload)
    return str(target)


def _synthesize_once(args: Tuple[str, str, str]) -> str:
    if args[2] and args[2] not in sys.path:
        sys.path.insert(0, args[2])
    from ydb.tools.include_sanitizer.analyze.header_probe import synthesize_probe
    probes_dir = Path(args[0])
    header_rel = args[1]
    probe = synthesize_probe(header_rel, Path(REPO_ROOT), probes_dir)
    return str(probe.probe_path)


class CacheStoreRaceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="ydb-iclean-race-"))

    def test_many_writers_same_key_no_enoent(self) -> None:
        target = self.tmp / "ab" / "abcdef0123456789.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps({"schema_version": 1, "file": "x", "kind": "header"})

        N = 32
        errors = []
        with ProcessPoolExecutor(max_workers=N) as ex:
            futures = [
                ex.submit(_store_once, (str(target), payload, str(_REPO)))
                for _ in range(N)
            ]
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as e:
                    errors.append(repr(e))

        self.assertFalse(errors, f"workers raised: {errors}")
        self.assertTrue(target.exists(), "expected canonical cache file")
        stray = list(target.parent.glob(f"{target.name}.tmp.*"))
        self.assertFalse(stray, f"left stray temp files: {stray}")
        loaded = json.loads(target.read_text(encoding="utf-8"))
        self.assertEqual(loaded["file"], "x")


class SynthesizeProbeRaceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="ydb-iclean-probe-race-"))

    def test_many_workers_same_probe(self) -> None:
        N = 32
        header_rel = "ydb/core/base/defs.h"
        errors = []
        results = set()
        with ProcessPoolExecutor(max_workers=N) as ex:
            futures = [
                ex.submit(_synthesize_once, (str(self.tmp), header_rel, str(_REPO)))
                for _ in range(N)
            ]
            for fut in as_completed(futures):
                try:
                    results.add(fut.result())
                except Exception as e:
                    errors.append(repr(e))

        self.assertFalse(errors, f"workers raised: {errors}")
        self.assertEqual(len(results), 1, f"unexpected probe paths: {results}")
        probe_path = Path(next(iter(results)))
        self.assertTrue(probe_path.exists())
        self.assertIn(header_rel, probe_path.read_text(encoding="utf-8"))
        stray = list(self.tmp.glob(f"{probe_path.stem}.cpp.tmp.*"))
        self.assertFalse(stray, f"left stray temp files: {stray}")


if __name__ == "__main__":
    unittest.main()
