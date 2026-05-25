"""Summary writer for stability tests.

When a test is started with ``--test-param summary-dir=<dir>``, this module
writes detected errors (coredumps, OOMs, sanitizer errors, VERIFY fails) and
the final test status (with exception/traceback, if any) into separate files
inside that directory.

Each test gets its own subdirectory, derived from PYTEST_CURRENT_TEST (or an
explicit name passed in), so that parametrized cases do not overwrite each
other.

File layout (under ``<summary-dir>/<test_name>/``):
    coredumps.txt      - one line per coredump (slot, core_id, core_hash, version)
    oom.txt            - one line per node that experienced OOM
    sanitizer.txt      - host header + sanitizer violation text per host
    verify.txt         - one line per node with VERIFY fail count
    workload_errors.txt- workload-level error messages (not host issues)
    status.txt         - final status: passed | failed | broken (+ message)
    exception.txt      - exception type/message/traceback if the test raised
"""
from __future__ import annotations

import logging
import os
import re
import traceback
from typing import Iterable, Optional

from ydb.tests.library.stability.utils.utils import get_external_param

LOGGER = logging.getLogger(__name__)


def _sanitize(name: str) -> str:
    """Make ``name`` safe for use as a filesystem path component."""
    if not name:
        return 'unknown_test'
    # Replace anything that's not alnum / dash / underscore / dot
    safe = re.sub(r'[^A-Za-z0-9._-]+', '_', name)
    # Trim leading/trailing underscores/dots
    safe = safe.strip('._')
    return safe or 'unknown_test'


def _current_test_name() -> str:
    """Derive a test name from PYTEST_CURRENT_TEST env var.

    PYTEST_CURRENT_TEST is in form ``path/to/test_file.py::TestCls::test_x[param] (call)``.
    We take everything before the trailing " (phase)" and use the last "::"
    chunk plus parameters.
    """
    raw = os.environ.get('PYTEST_CURRENT_TEST', '')
    if not raw:
        return 'unknown_test'
    # Strip " (call)" / " (setup)" / " (teardown)" suffix
    raw = raw.rsplit(' (', 1)[0]
    # Use last "::" piece to keep it short, otherwise full id
    parts = raw.split('::')
    name = '::'.join(parts[1:]) if len(parts) > 1 else raw
    return _sanitize(name)


class SummaryWriter:
    """Write per-test error reports into a structured summary directory.

    All methods silently no-op if no summary directory was configured.
    Any I/O errors are logged but never re-raised, so that summary writing
    never breaks the test itself.
    """

    def __init__(self, summary_dir: Optional[str] = None, test_name: Optional[str] = None):
        if summary_dir is None:
            summary_dir = get_external_param('summary-dir', None)
        self._enabled = bool(summary_dir)
        self._test_dir: Optional[str] = None

        if not self._enabled:
            return

        test_name = test_name or _current_test_name()
        self._test_dir = os.path.join(summary_dir, _sanitize(test_name))
        try:
            os.makedirs(self._test_dir, exist_ok=True)
            LOGGER.info(f"SummaryWriter: writing summary to {self._test_dir}")
        except OSError as exc:
            LOGGER.error(f"SummaryWriter: failed to create {self._test_dir}: {exc}")
            self._enabled = False
            self._test_dir = None

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def test_dir(self) -> Optional[str]:
        return self._test_dir

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def _write(self, filename: str, content: str, *, append: bool = False) -> None:
        if not self._enabled or not self._test_dir:
            return
        path = os.path.join(self._test_dir, filename)
        mode = 'a' if append else 'w'
        try:
            with open(path, mode, encoding='utf-8') as f:
                f.write(content)
                if not content.endswith('\n'):
                    f.write('\n')
        except OSError as exc:
            LOGGER.error(f"SummaryWriter: failed to write {path}: {exc}")

    def _append_lines(self, filename: str, lines: Iterable[str]) -> None:
        text = '\n'.join(lines)
        if text:
            self._write(filename, text, append=True)

    # ------------------------------------------------------------------
    # High-level API: write specific error categories
    # ------------------------------------------------------------------

    def write_node_errors(self, node_errors) -> None:
        """Write node errors classified by type into separate files.

        Args:
            node_errors: iterable of NodeErrors objects (see allure_utils.NodeErrors)
        """
        if not self._enabled or not node_errors:
            return

        coredump_lines: list[str] = []
        oom_lines: list[str] = []
        verify_lines: list[str] = []
        sanitizer_blocks: list[str] = []

        for ne in node_errors:
            node = getattr(ne, 'node', None)
            slot = getattr(node, 'slot', '?')
            host = getattr(node, 'host', '?')

            core_hashes = getattr(ne, 'core_hashes', None) or []
            for core in core_hashes:
                # core may be a (core_id, core_hash, version) tuple
                try:
                    core_id, core_hash, version = core
                except (TypeError, ValueError):
                    core_id, core_hash, version = core, '', ''
                coredump_lines.append(
                    f"slot={slot} host={host} core_id={core_id} core_hash={core_hash} version={version}"
                )

            if getattr(ne, 'was_oom', False):
                oom_lines.append(f"slot={slot} host={host} experienced OOM")

            verifies = getattr(ne, 'verifies', 0) or 0
            if verifies:
                verify_lines.append(f"host={host} slot={slot} verify_fails={verifies}")

            san_errors = getattr(ne, 'sanitizer_errors', 0) or 0
            if san_errors:
                san_output = getattr(ne, 'sanitizer_output', None) or ''
                block = (
                    f"===== host={host} slot={slot} sanitizer_errors={san_errors} =====\n"
                    f"{san_output}"
                )
                sanitizer_blocks.append(block)

        if coredump_lines:
            self._append_lines('coredumps.txt', coredump_lines)
        if oom_lines:
            self._append_lines('oom.txt', oom_lines)
        if verify_lines:
            self._append_lines('verify.txt', verify_lines)
        if sanitizer_blocks:
            self._append_lines('sanitizer.txt', sanitizer_blocks)

    def write_workload_errors(self, errors: Iterable[str]) -> None:
        """Write workload-level error messages (not tied to a particular host)."""
        if not self._enabled:
            return
        errors = [e for e in (errors or []) if e]
        if errors:
            self._append_lines('workload_errors.txt', errors)

    def write_status(self, status: str, message: str = '') -> None:
        """Write final test status: passed | failed | broken.

        Args:
            status: one of 'passed', 'failed', 'broken'
            message: optional context message
        """
        if not self._enabled:
            return
        text = status if not message else f"{status}\n{message}"
        self._write('status.txt', text, append=False)

    def write_exception(self, exc: BaseException) -> None:
        """Write the exception type, message and traceback of a failure."""
        if not self._enabled:
            return
        try:
            tb_text = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        except Exception:
            tb_text = f"{type(exc).__name__}: {exc}\n"
        content = f"{type(exc).__name__}: {exc}\n\n{tb_text}"
        self._write('exception.txt', content, append=False)
