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
    oom.txt            - per-node OOM summary + full dmesg/OOM kernel messages
    sanitizer.txt      - per-host sanitizer summary + full sanitizer violation text
    verify.txt         - per-node VERIFY fail count + full VERIFY stacktraces
    workload_errors.txt- workload-level error messages (not host issues)
    warden_violations.txt - cluster-wide warden checks with violations (full text)
    warden_errors.txt  - cluster-wide warden checks in an infra-error state
    status.txt         - final status: passed | failed | broken (+ message)
    exception.txt      - exception type/message/traceback if the test raised

Per-type files (``oom.txt``, ``sanitizer.txt``, ``verify.txt``) receive both the
short per-node summary (from ``write_node_errors``) and the full error text (from
``write_warden_results``), so each error type has a single self-contained file.
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

    def write_warden_results(self, warden_results) -> None:
        """Write the *full text* of every non-OK warden check into per-type files.

        ``write_node_errors`` only records per-host *counts* (taken from the
        aggregated ``NodeErrors`` objects); it does NOT contain the actual error
        messages.  The full violation text (VERIFY stacktraces, sanitizer dumps,
        OOM kernel messages, etc.) is only available here, in
        ``warden_results.checks[...].violations``.

        Each error type is routed to its own file so that the full error text is
        always persisted:
            verify.txt    - VERIFY-failed checks (full stacktraces)
            sanitizer.txt - sanitizer checks (full sanitizer output)
            oom.txt       - dmesg/OOM checks (full kernel messages)
            warden_violations.txt - other cluster-wide checks with violations
            warden_errors.txt     - cluster-wide checks in an infra-error state

        Per-host count summaries written by ``write_node_errors`` are kept; the
        full-text blocks produced here are appended to the same type files, so a
        single file per error type contains both the summary and full details.

        Args:
            warden_results: ``WardenResults`` object from the orchestrator.
        """
        if not self._enabled or warden_results is None:
            return
        verify_blocks: list[str] = []
        sanitizer_blocks: list[str] = []
        oom_blocks: list[str] = []
        violation_blocks: list[str] = []
        error_blocks: list[str] = []
        for name, check in warden_results.checks.items():
            if check.is_ok():
                continue
            name_lower = name.lower()
            affected = ', '.join(sorted(check.affected_hosts)) if check.affected_hosts else '—'
            header = (
                f"===== {name} [{check.status}] hosts={affected} "
                f"violations={len(check.violations)} ====="
            )
            body = '\n'.join(str(v) for v in check.violations) if check.violations else ''
            block = header if not body else f"{header}\n{body}"

            if 'verify' in name_lower and 'failed' in name_lower:
                verify_blocks.append(block)
            elif 'sanitizer' in name_lower:
                sanitizer_blocks.append(block)
            elif 'grepdmesg' in name_lower or 'grep_dmesg' in name_lower:
                oom_blocks.append(block)
            elif check.is_violation():
                violation_blocks.append(block)
            else:
                error_blocks.append(block)

        if verify_blocks:
            self._append_lines('verify.txt', verify_blocks)
        if sanitizer_blocks:
            self._append_lines('sanitizer.txt', sanitizer_blocks)
        if oom_blocks:
            self._append_lines('oom.txt', oom_blocks)
        if violation_blocks:
            self._append_lines('warden_violations.txt', violation_blocks)
        if error_blocks:
            self._append_lines('warden_errors.txt', error_blocks)
        if not warden_results.poll_success and warden_results.error_message:
            self._append_lines('warden_errors.txt', [
                f"warden_polling: {warden_results.error_message}"
            ])

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
