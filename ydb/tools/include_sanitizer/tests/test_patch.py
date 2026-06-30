"""Tests for the wrapper-mode patching of build/scripts/*.py.

These do NOT modify the real build scripts. They make temp copies and
verify the patch + unpatch cycle leaves the file byte-identical to the
original, that the shim is invoked before the actual compile call, and
that the shim writes a JSON entry to ``YDB_COMPDB_DIR``.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
_REPO = HERE.parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from ydb.tools.include_sanitizer.common import REPO_ROOT  # noqa: E402
from ydb.tools.include_sanitizer.compdb.generate import (  # noqa: E402
    PATCH_MARKER_BEGIN,
    PATCH_MARKER_END,
    patch_clang_wrapper,
    patch_retry_cc,
    unpatch_clang_wrapper,
    unpatch_retry_cc,
)

import ast as _ast  # noqa: E402


def _const_str(node):
    """String value of an ast string constant (compat: ast.Str on <3.8)."""
    if isinstance(node, _ast.Constant) and isinstance(node.value, str):
        return node.value
    # Python < 3.8 used ast.Str with a .s attribute.
    if node.__class__.__name__ == "Str":
        return getattr(node, "s", None)
    return None


def _find_main_if(tree):
    """Find the top-level ``if __name__ == '__main__':`` If node.

    Avoids ast.unparse (Python 3.9+) by inspecting node types.
    """
    for stmt in tree.body:
        if not isinstance(stmt, _ast.If):
            continue
        t = stmt.test
        if (isinstance(t, _ast.Compare)
                and isinstance(t.left, _ast.Name)
                and t.left.id == "__name__"):
            for c in t.comparators:
                if _const_str(c) == "__main__":
                    return stmt
    return None


# These tests patch the real build/scripts/*.py, which are part of the
# source checkout but not available in a hermetic CI test sandbox. Skip
# them there; they run fully from a source checkout.
_HAS_BUILD_SCRIPTS = (REPO_ROOT / "build" / "scripts" / "retry_cc.py").exists()


@unittest.skipUnless(_HAS_BUILD_SCRIPTS, "needs source build/scripts/*.py")
class WrapperPatchRoundtripTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="ydb-iclean-patch-"))
        src_clang = REPO_ROOT / "build" / "scripts" / "clang_wrapper.py"
        src_retry = REPO_ROOT / "build" / "scripts" / "retry_cc.py"
        self.clang = self.tmp / "clang_wrapper.py"
        self.retry = self.tmp / "retry_cc.py"
        shutil.copy2(src_clang, self.clang)
        shutil.copy2(src_retry, self.retry)
        self.clang_original = self.clang.read_bytes()
        self.retry_original = self.retry.read_bytes()

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_patch_clang_wrapper_then_unpatch_byte_identical(self) -> None:
        patch_clang_wrapper(self.clang)
        patched = self.clang.read_text(encoding="utf-8")
        self.assertIn(PATCH_MARKER_BEGIN, patched)
        self.assertIn(PATCH_MARKER_END, patched)
        unpatch_clang_wrapper(self.clang)
        self.assertEqual(self.clang.read_bytes(), self.clang_original)

    def test_patch_retry_cc_then_unpatch_byte_identical(self) -> None:
        patch_retry_cc(self.retry)
        patched = self.retry.read_text(encoding="utf-8")
        self.assertIn(PATCH_MARKER_BEGIN, patched)
        self.assertIn(PATCH_MARKER_END, patched)
        unpatch_retry_cc(self.retry)
        self.assertEqual(self.retry.read_bytes(), self.retry_original)

    def test_patch_is_idempotent(self) -> None:
        patch_clang_wrapper(self.clang)
        once = self.clang.read_text(encoding="utf-8")
        patch_clang_wrapper(self.clang)
        twice = self.clang.read_text(encoding="utf-8")
        self.assertEqual(once, twice)


@unittest.skipUnless(_HAS_BUILD_SCRIPTS, "needs source build/scripts/*.py")
class RecorderShimExecutionTest(unittest.TestCase):
    """Actually invoke the patched retry_cc.py with a fake compiler.

    We patch ``retry_cc.py`` into a temp dir, run it with a small Python
    shim that pretends to be the compiler (just exits 0), and then verify
    that the recorder wrote a JSON entry.
    """

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="ydb-iclean-shim-"))
        src_retry = REPO_ROOT / "build" / "scripts" / "retry_cc.py"
        self.retry = self.tmp / "retry_cc.py"
        shutil.copy2(src_retry, self.retry)
        patch_retry_cc(self.retry)

        self.fake_cc = self.tmp / "fake_cc.py"
        self.fake_cc.write_text(
            "#!/usr/bin/env python3\nimport sys\nsys.exit(0)\n",
            encoding="utf-8",
        )
        self.fake_cc.chmod(0o755)

        self.entries_dir = self.tmp / "entries"
        self.entries_dir.mkdir()

        # The fake source file the "compiler" pretends to process
        self.src = self.tmp / "x.cpp"
        self.src.write_text("int main(){}\n", encoding="utf-8")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_shim_records_and_compile_succeeds(self) -> None:
        """The recorder must fire AND the compile must succeed.

        With ``YDB_COMPDB_DIR`` set, the shim records the command and
        ``execv``s the compiler directly, bypassing retry_cc.py's
        Py3-buggy retry loop. The fake compiler exits 0 so retry_cc.py
        as a whole returns 0.
        """
        env = os.environ.copy()
        env["YDB_COMPDB_DIR"] = str(self.entries_dir)

        cmd = [
            sys.executable, str(self.retry),
            sys.executable, str(self.fake_cc),
            "-c", str(self.src),
            "-o", str(self.tmp / "x.o"),
            "-I/some/include",
            "-DFOO=1",
        ]
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0,
                         msg=f"retry_cc exited {proc.returncode}: {proc.stderr}")

        entries = list(self.entries_dir.glob("*.json"))
        self.assertEqual(len(entries), 1, msg=f"expected 1 entry, got {entries}")
        data = json.loads(entries[0].read_text(encoding="utf-8"))
        self.assertEqual(data["file"], str(self.src))
        self.assertIn("-I/some/include", data["arguments"])
        self.assertIn("-DFOO=1", data["arguments"])
        # arguments[0] is whatever retry_cc.py saw as the "compiler"
        # binary (the first argv slot after itself). In a real ya build
        # this is the absolute path to clang/clang++; in this test it
        # happens to be the Python interpreter wrapping the fake_cc.
        self.assertIn(str(self.fake_cc), " ".join(data["arguments"]))

    def test_shim_invokes_inline_analyze_when_requested(self) -> None:
        """With YDB_ANALYZE_INLINE=1 the shim must populate the per-TU cache.

        We use a tmp .cpp source under the workspace (so analyze_tu's
        repo_relative logic accepts it) and stub clang-include-cleaner
        with a python script that exits 0 silently. The success
        criterion is a new per-TU JSON file written for our source.
        """
        stub_cleaner = self.tmp / "stub_cleaner.py"
        stub_cleaner.write_text(
            "#!/usr/bin/env python3\nimport sys\nsys.exit(0)\n",
            encoding="utf-8",
        )
        stub_cleaner.chmod(0o755)

        from ydb.tools.include_sanitizer.common import PATHS
        cache_per_tu = PATHS.per_tu_dir
        before = set(cache_per_tu.glob("**/*.json")) if cache_per_tu.exists() else set()

        # Materialize a unique .cpp under the workspace so we don't
        # collide with any cached entry from a previous run.
        src_dir = PATHS.cache_dir / "test_sources"
        src_dir.mkdir(parents=True, exist_ok=True)
        src_under_repo = src_dir / f"shim_smoke_{os.getpid()}_{int(__import__('time').time() * 1000)}.cpp"
        src_under_repo.write_text("int main(){}\n", encoding="utf-8")

        try:
            env = os.environ.copy()
            env["YDB_COMPDB_DIR"] = str(self.entries_dir)
            env["YDB_ANALYZE_INLINE"] = "1"
            env["YDB_CLEANER_BIN"] = str(stub_cleaner)

            cmd = [
                sys.executable, str(self.retry),
                sys.executable, str(self.fake_cc),
                "-c", str(src_under_repo),
                "-o", str(self.tmp / "x.o"),
            ]
            proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)

            after = set(cache_per_tu.glob("**/*.json")) if cache_per_tu.exists() else set()
            new_entries = after - before
            for p in new_entries:
                try:
                    p.unlink()
                except OSError:
                    pass
            self.assertTrue(
                new_entries,
                f"inline-analyze did not write to {cache_per_tu}: "
                f"stderr={proc.stderr!r}, stdout={proc.stdout!r}",
            )
        finally:
            try:
                src_under_repo.unlink()
            except OSError:
                pass

    def test_shim_bypasses_buggy_retry_on_empty_output(self) -> None:
        """Regression for the user-reported crash.

        The upstream ``retry_cc.py`` raises ``TypeError: a bytes-like
        object is required, not 'str'`` in ``need_retry()`` when the
        compiler succeeds with empty output. In compdb-recording mode
        our shim must avoid invoking that retry path at all (it
        ``execv``s the compiler directly).
        """
        env = os.environ.copy()
        env["YDB_COMPDB_DIR"] = str(self.entries_dir)

        # fake_cc emits NO output and exits 0 - the exact condition
        # that trips the upstream bug.
        silent_cc = self.tmp / "silent_cc.py"
        silent_cc.write_text(
            "#!/usr/bin/env python3\nimport sys\nsys.exit(0)\n",
            encoding="utf-8",
        )
        silent_cc.chmod(0o755)

        cmd = [
            sys.executable, str(self.retry),
            sys.executable, str(silent_cc),
            "-c", str(self.src),
            "-o", str(self.tmp / "x.o"),
        ]
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertNotIn("TypeError", proc.stderr)
        self.assertNotIn("need_retry", proc.stderr)
        self.assertEqual(len(list(self.entries_dir.glob("*.json"))), 1)

    def test_shim_no_op_without_env(self) -> None:
        """Without ``YDB_COMPDB_DIR`` the shim must not record anything.

        Note: retry_cc.py upstream has a pre-existing Py3 quirk that
        makes it raise on empty compiler output. We only assert that no
        compdb entry was written — that is the correctness property our
        shim is responsible for.
        """
        env = os.environ.copy()
        env.pop("YDB_COMPDB_DIR", None)
        cmd = [
            sys.executable, str(self.retry),
            sys.executable, str(self.fake_cc),
            "-c", str(self.src),
            "-o", str(self.tmp / "x.o"),
        ]
        subprocess.run(cmd, env=env, capture_output=True, text=True)
        self.assertEqual(list(self.entries_dir.glob("*.json")), [])

    def test_shim_does_not_swallow_compile_dispatch(self) -> None:
        """Regression: the shim must be INSIDE the ``__main__`` block.

        If it is dedented, Python will (silently) re-parse the original
        ``if '-c' in cmd:`` dispatch as the body of the shim's
        ``if YDB_COMPDB_DIR:``, which means with the env var unset the
        actual compile is skipped. Catch that by inspecting the AST
        (without ast.unparse, which is Python 3.9+).
        """
        import ast
        tree = ast.parse(self.retry.read_text(encoding="utf-8"))
        main_if = _find_main_if(tree)
        self.assertIsNotNone(main_if, "patched retry_cc lost its __main__ block")

        def contains_compile_dispatch(node) -> bool:
            for child in ast.walk(node):
                if isinstance(child, ast.If) and isinstance(child.test, ast.Compare):
                    for sub in ast.walk(child.test):
                        if _const_str(sub) == "-c":
                            return True
            return False

        self.assertTrue(
            contains_compile_dispatch(main_if),
            "the `if '-c' in cmd:` dispatch must remain inside __main__",
        )

    def test_clang_wrapper_subprocess_call_still_in_main(self) -> None:
        """Same regression check for clang_wrapper.py."""
        import ast
        wrapper = self.tmp / "clang_wrapper.py"
        shutil.copy2(REPO_ROOT / "build" / "scripts" / "clang_wrapper.py", wrapper)
        from ydb.tools.include_sanitizer.compdb.generate import patch_clang_wrapper
        patch_clang_wrapper(wrapper)

        tree = ast.parse(wrapper.read_text(encoding="utf-8"))
        main_if = _find_main_if(tree)
        self.assertIsNotNone(main_if)

        found_call = False
        for node in ast.walk(main_if):
            if (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "call"
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "subprocess"):
                found_call = True
                break
        self.assertTrue(found_call,
                        "subprocess.call must remain inside clang_wrapper __main__")


class DetectSelfBinaryTest(unittest.TestCase):
    """Exercise detect_self_binary() by stubbing sys.executable.

    We can't assert on the ambient sys.executable: from source it is a
    python interpreter, but inside a ya PY3TEST it is the bundled test
    binary (a non-python executable), which legitimately self-routes.
    """

    def test_python_interpreter_is_not_a_binary(self) -> None:
        from ydb.tools.include_sanitizer.compdb import generate
        saved = sys.executable
        try:
            sys.executable = "/usr/bin/python3"
            self.assertIsNone(generate.detect_self_binary())
            sys.executable = "/opt/pypy3/bin/pypy3"
            self.assertIsNone(generate.detect_self_binary())
        finally:
            sys.executable = saved

    def test_bundled_binary_is_detected(self) -> None:
        from ydb.tools.include_sanitizer.compdb import generate
        tmp = Path(tempfile.mkdtemp(prefix="ydb-iclean-selfbin-"))
        try:
            fake_bin = tmp / "include-sanitizer"
            fake_bin.write_text("#!/bin/sh\n", encoding="utf-8")
            fake_bin.chmod(0o755)
            saved = sys.executable
            try:
                sys.executable = str(fake_bin)
                self.assertEqual(generate.detect_self_binary(),
                                 os.path.realpath(str(fake_bin)))
            finally:
                sys.executable = saved
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class BinaryShimTemplateTest(unittest.TestCase):
    def test_recorder_bin_bakes_binary_exec_shim(self) -> None:
        """patch_retry_cc(recorder_bin=...) must emit a binary-exec shim."""
        from ydb.tools.include_sanitizer.compdb.generate import _shim_block, SHIM_TEMPLATE_RETRY_CC_BIN
        block = _shim_block(SHIM_TEMPLATE_RETRY_CC_BIN, recorder_bin="/opt/include-sanitizer")
        self.assertIn("__shim", block)
        self.assertIn("/opt/include-sanitizer", block)
        self.assertIn("execv", block)
        # No source-tree import in binary mode.
        self.assertNotIn("import", block.split("execv")[0].replace("import os", ""))


@unittest.skipUnless(_HAS_BUILD_SCRIPTS, "needs source build/scripts/*.py")
class BinaryRoutedShimExecutionTest(unittest.TestCase):
    """End-to-end: the binary-routed shim execs a stub 'binary' that runs
    the hidden ``__shim`` CLI entry, which records and execs the compiler."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="ydb-iclean-binshim-"))
        src_retry = REPO_ROOT / "build" / "scripts" / "retry_cc.py"
        self.retry = self.tmp / "retry_cc.py"
        shutil.copy2(src_retry, self.retry)

        # Stub that stands in for the bundled include-sanitizer binary:
        # `<stub> __shim <compiler> <args>` routes into cli.main.
        self.stub_bin = self.tmp / "include-sanitizer"
        self.stub_bin.write_text(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            f"sys.path.insert(0, {str(_REPO)!r})\n"
            "from ydb.tools.include_sanitizer.cli import main\n"
            "sys.exit(main(sys.argv[1:]))\n",
            encoding="utf-8",
        )
        self.stub_bin.chmod(0o755)

        patch_retry_cc(self.retry, recorder_bin=str(self.stub_bin))

        self.fake_cc = self.tmp / "fake_cc.py"
        self.fake_cc.write_text(
            "#!/usr/bin/env python3\nimport sys\nsys.exit(0)\n",
            encoding="utf-8",
        )
        self.fake_cc.chmod(0o755)

        self.entries_dir = self.tmp / "entries"
        self.entries_dir.mkdir()
        self.src = self.tmp / "x.cpp"
        self.src.write_text("int main(){}\n", encoding="utf-8")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_binary_routed_shim_records_via_cli(self) -> None:
        env = os.environ.copy()
        env["YDB_COMPDB_DIR"] = str(self.entries_dir)
        cmd = [
            sys.executable, str(self.retry),
            sys.executable, str(self.fake_cc),
            "-c", str(self.src),
            "-o", str(self.tmp / "x.o"),
            "-DBAR=2",
        ]
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0,
                         msg=f"exited {proc.returncode}: {proc.stderr}")
        entries = list(self.entries_dir.glob("*.json"))
        self.assertEqual(len(entries), 1, msg=f"expected 1 entry, got {entries}")
        data = json.loads(entries[0].read_text(encoding="utf-8"))
        self.assertEqual(data["file"], str(self.src))
        self.assertIn("-DBAR=2", data["arguments"])


if __name__ == "__main__":
    unittest.main()
