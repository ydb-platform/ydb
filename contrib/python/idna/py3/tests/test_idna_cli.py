#!/usr/bin/env python

import io
import subprocess
import sys
import unittest
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from unittest import mock

from idna import cli
from idna.package_data import __version__


def run_cli(*argv, stdin=None):
    """Invoke ``cli.main`` with ``argv`` and capture (rc, stdout, stderr).

    Pass ``stdin`` as a string to simulate a piped stdin; omit it to
    simulate an interactive terminal.
    """
    out = io.StringIO()
    err = io.StringIO()
    with redirect_stdout(out), redirect_stderr(err), _patched_stdin(stdin):
        rc = cli.main(list(argv))
    return rc, out.getvalue(), err.getvalue()


class _FakeStdin(io.StringIO):
    """``io.StringIO`` with a configurable ``isatty`` result."""

    def __init__(self, data: str, is_tty: bool) -> None:
        super().__init__(data)
        self._is_tty = is_tty

    def isatty(self) -> bool:
        return self._is_tty


@contextmanager
def _patched_stdin(data):
    """Replace ``sys.stdin`` with a fake stream and isatty() result."""
    stream = _FakeStdin("" if data is None else data, is_tty=data is None)
    with mock.patch.object(sys, "stdin", stream):
        yield


class CLIAutoDetectTests(unittest.TestCase):
    def test_autodetect_encodes_unicode_input(self):
        rc, out, err = run_cli("пример.рф")
        self.assertEqual(rc, 0)
        self.assertEqual(out.strip(), "xn--e1afmkfd.xn--p1ai")
        self.assertEqual(err, "")

    def test_autodetect_decodes_alabel(self):
        rc, out, _ = run_cli("xn--e1afmkfd.xn--p1ai")
        self.assertEqual(rc, 0)
        self.assertEqual(out.strip(), "пример.рф")

    def test_autodetect_decodes_when_any_label_is_alabel(self):
        # Mixed label: at least one xn-- label triggers decode mode.
        rc, out, _ = run_cli("xn--11b5bs3a9aj6g.example")
        self.assertEqual(rc, 0)
        self.assertEqual(out.strip(), "परीक्षा.example")

    def test_autodetect_encodes_plain_ascii_input(self):
        rc, out, _ = run_cli("example.com")
        self.assertEqual(rc, 0)
        self.assertEqual(out.strip(), "example.com")

    def test_looks_like_alabel_is_case_insensitive(self):
        self.assertTrue(cli._looks_like_alabel("XN--ZCKZAH"))
        self.assertTrue(cli._looks_like_alabel("foo.Xn--ZcKzAh"))
        self.assertFalse(cli._looks_like_alabel("example.com"))
        self.assertFalse(cli._looks_like_alabel("xnfoo.example"))

    def test_looks_like_alabel_handles_unicode_dots(self):
        # IDEOGRAPHIC FULL STOP should split labels for the purpose of
        # spotting an A-label, matching the behavior of idna.encode/decode.
        self.assertTrue(cli._looks_like_alabel("foo。xn--zckzah"))


class CLIExplicitModeTests(unittest.TestCase):
    def test_encode_flag_forces_encode(self):
        rc, out, _ = run_cli("-e", "παράδειγμα")
        self.assertEqual(rc, 0)
        self.assertEqual(out.strip(), "xn--hxajbheg2az3al")

    def test_long_encode_flag_forces_encode(self):
        rc, out, _ = run_cli("--encode", "한국")
        self.assertEqual(rc, 0)
        self.assertEqual(out.strip(), "xn--3e0b707e")

    def test_decode_flag_forces_decode(self):
        rc, out, _ = run_cli("-d", "xn--hxajbheg2az3al")
        self.assertEqual(rc, 0)
        self.assertEqual(out.strip(), "παράδειγμα")

    def test_long_decode_flag_forces_decode(self):
        rc, out, _ = run_cli("--decode", "xn--3e0b707e")
        self.assertEqual(rc, 0)
        self.assertEqual(out.strip(), "한국")

    def test_encode_and_decode_are_mutually_exclusive(self):
        # argparse exits via SystemExit (code 2) for mutually-exclusive errors.
        with self.assertRaises(SystemExit) as ctx, redirect_stderr(io.StringIO()):
            cli.main(["-e", "-d", "example.com"])
        self.assertEqual(ctx.exception.code, 2)


class CLIUTS46Tests(unittest.TestCase):
    def test_uts46_is_applied_by_default(self):
        # Uppercase Greek is not PVALID under IDNA 2008 alone; UTS #46
        # case-folds it so encoding succeeds.
        rc, out, _ = run_cli("ΠΑΡΆΔΕΙΓΜΑ.ΕΛ")
        self.assertEqual(rc, 0)
        self.assertEqual(out.strip(), "xn--hxajbheg2az3al.xn--qxam")

    def test_strict_disables_uts46(self):
        rc, out, err = run_cli("--strict", "ΠΑΡΆΔΕΙΓΜΑ.ΕΛ")
        self.assertEqual(rc, 1)
        self.assertEqual(out, "")
        self.assertIn("encode failed", err)
        self.assertIn("U+03A0", err)

    def test_strict_does_not_affect_valid_inputs(self):
        rc, out, _ = run_cli("--strict", "пример.рф")
        self.assertEqual(rc, 0)
        self.assertEqual(out.strip(), "xn--e1afmkfd.xn--p1ai")

    def test_uts46_applies_to_explicit_decode(self):
        # Trailing FULLWIDTH FULL STOP (U+FF0E) is mapped to "." by UTS #46
        # so the input still decodes as a single A-label.
        rc, out, _ = run_cli("-d", "xn--zckzah．")
        self.assertEqual(rc, 0)
        self.assertEqual(out.strip(), "テスト.")


class CLIErrorHandlingTests(unittest.TestCase):
    def test_invalid_input_returns_nonzero_and_writes_to_stderr(self):
        rc, out, err = run_cli("foo_bar")
        self.assertEqual(rc, 1)
        self.assertEqual(out, "")
        self.assertTrue(err.startswith("idna: encode failed for 'foo_bar':"))

    def test_invalid_alabel_decode_returns_nonzero(self):
        # Trailing hyphen after the xn-- prefix is rejected by ulabel().
        rc, out, err = run_cli("-d", "xn--")
        self.assertEqual(rc, 1)
        self.assertEqual(out, "")
        self.assertIn("decode failed", err)

    def test_missing_argument_with_tty_stdin_is_an_argparse_error(self):
        # Interactive shell (no piped stdin) and no domain arg → argparse exits 2.
        with self.assertRaises(SystemExit) as ctx, redirect_stderr(io.StringIO()), _patched_stdin(None):
            cli.main([])
        self.assertEqual(ctx.exception.code, 2)


class CLIVersionTests(unittest.TestCase):
    def test_version_flag(self):
        buf = io.StringIO()
        with self.assertRaises(SystemExit) as ctx, redirect_stdout(buf):
            cli.main(["--version"])
        self.assertEqual(ctx.exception.code, 0)
        self.assertIn(__version__, buf.getvalue())


class CLIMultipleDomainsTests(unittest.TestCase):
    def test_multiple_unicode_positional_domains_are_all_encoded(self):
        rc, out, _ = run_cli("пример.рф", "παράδειγμα", "한국")
        self.assertEqual(rc, 0)
        self.assertEqual(
            out.splitlines(),
            ["xn--e1afmkfd.xn--p1ai", "xn--hxajbheg2az3al", "xn--3e0b707e"],
        )

    def test_multiple_alabel_positional_domains_are_all_decoded(self):
        rc, out, _ = run_cli("xn--e1afmkfd.xn--p1ai", "xn--hxajbheg2az3al", "xn--3e0b707e")
        self.assertEqual(rc, 0)
        self.assertEqual(out.splitlines(), ["пример.рф", "παράδειγμα", "한국"])

    def test_mode_flag_applies_to_every_positional_domain(self):
        rc, out, _ = run_cli("-e", "한국", "παράδειγμα")
        self.assertEqual(rc, 0)
        self.assertEqual(
            out.splitlines(),
            ["xn--3e0b707e", "xn--hxajbheg2az3al"],
        )

    def test_failures_are_reported_per_domain_and_processing_continues(self):
        rc, out, err = run_cli("пример.рф", "foo_bar", "παράδειγμα")
        self.assertEqual(rc, 1)
        self.assertEqual(
            out.splitlines(),
            ["xn--e1afmkfd.xn--p1ai", "xn--hxajbheg2az3al"],
        )
        self.assertIn("encode failed for 'foo_bar'", err)


class CLIModeLockTests(unittest.TestCase):
    """The first input picks the mode; the rest of the stream follows."""

    def test_first_unicode_input_locks_subsequent_inputs_to_encode(self):
        # 'foo_bar' is invalid in either direction; the stderr mode word
        # reveals which mode the second input was processed in.
        rc, out, err = run_cli("пример.рф", "foo_bar")
        self.assertEqual(rc, 1)
        self.assertEqual(out.splitlines(), ["xn--e1afmkfd.xn--p1ai"])
        self.assertIn("encode failed for 'foo_bar'", err)

    def test_first_alabel_input_locks_subsequent_inputs_to_decode(self):
        rc, out, err = run_cli("xn--zckzah", "foo_bar")
        self.assertEqual(rc, 1)
        self.assertEqual(out.splitlines(), ["テスト"])
        self.assertIn("decode failed for 'foo_bar'", err)

    def test_explicit_mode_flag_overrides_first_input_heuristic(self):
        # First input looks like an A-label but -e forces encode for everything.
        rc, out, err = run_cli("-e", "xn--zckzah", "foo_bar")
        self.assertEqual(rc, 1)
        self.assertEqual(out.splitlines(), ["xn--zckzah"])
        self.assertIn("encode failed for 'foo_bar'", err)

    def test_stdin_first_line_locks_mode_for_the_stream(self):
        rc, out, err = run_cli(stdin="xn--zckzah\nfoo_bar\n")
        self.assertEqual(rc, 1)
        self.assertEqual(out.splitlines(), ["テスト"])
        self.assertIn("decode failed for 'foo_bar'", err)


class CLIStdinTests(unittest.TestCase):
    def test_reads_one_domain_per_line_from_piped_stdin(self):
        rc, out, _ = run_cli(stdin="пример.рф\nπαράδειγμα\n한국\n")
        self.assertEqual(rc, 0)
        self.assertEqual(
            out.splitlines(),
            ["xn--e1afmkfd.xn--p1ai", "xn--hxajbheg2az3al", "xn--3e0b707e"],
        )

    def test_blank_lines_in_stdin_are_skipped(self):
        rc, out, _ = run_cli(stdin="\nпример.рф\n\n   \nπαράδειγμα\n")
        self.assertEqual(rc, 0)
        self.assertEqual(
            out.splitlines(),
            ["xn--e1afmkfd.xn--p1ai", "xn--hxajbheg2az3al"],
        )

    def test_empty_stdin_exits_cleanly_with_no_output(self):
        rc, out, err = run_cli(stdin="")
        self.assertEqual(rc, 0)
        self.assertEqual(out, "")
        self.assertEqual(err, "")

    def test_positional_domains_take_precedence_over_piped_stdin(self):
        rc, out, _ = run_cli("example.com", stdin="ignored.example\n")
        self.assertEqual(rc, 0)
        self.assertEqual(out.splitlines(), ["example.com"])

    def test_stdin_errors_continue_processing_and_set_exit_code(self):
        rc, out, err = run_cli(stdin="пример.рф\nfoo_bar\nπαράδειγμα\n")
        self.assertEqual(rc, 1)
        self.assertEqual(
            out.splitlines(),
            ["xn--e1afmkfd.xn--p1ai", "xn--hxajbheg2az3al"],
        )
        self.assertIn("encode failed for 'foo_bar'", err)

    def test_decode_flag_applies_to_every_stdin_line(self):
        rc, out, _ = run_cli("-d", stdin="xn--hxajbheg2az3al\nxn--3e0b707e\n")
        self.assertEqual(rc, 0)
        self.assertEqual(out.splitlines(), ["παράδειγμα", "한국"])


class CLIModuleEntryTests(unittest.TestCase):
    def test_python_dash_m_idna_runs_cli(self):
        # End-to-end: spawn ``python -m idna`` and check the round trip.
        result = subprocess.run(
            [sys.executable, "-m", "idna", "xn--zckzah"],
            check=False,
            capture_output=True,
            text=True,
            env={"Y_PYTHON_ENTRY_POINT": ":main"},
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "テスト")
        self.assertEqual(result.stderr, "")

    def test_python_dash_m_idna_reads_piped_stdin(self):
        # End-to-end: pipe a list of A-labels and confirm they all decode.
        result = subprocess.run(
            [sys.executable, "-m", "idna"],
            input="xn--e1afmkfd.xn--p1ai\nxn--zckzah\n",
            check=False,
            capture_output=True,
            text=True,
            env={"Y_PYTHON_ENTRY_POINT": ":main"},
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(
            result.stdout.splitlines(),
            ["пример.рф", "テスト"],
        )
        self.assertEqual(result.stderr, "")

    def test_python_dash_m_idna_reports_errors(self):
        result = subprocess.run(
            [sys.executable, "-m", "idna", "foo_bar"],
            check=False,
            capture_output=True,
            text=True,
            env={"Y_PYTHON_ENTRY_POINT": ":main"},
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertIn("encode failed", result.stderr)


if __name__ == "__main__":
    unittest.main()
