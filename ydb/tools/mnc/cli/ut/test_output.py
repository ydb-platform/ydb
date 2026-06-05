"""
Unit tests for output module.
"""

import unittest

from ydb.tools.mnc.lib.output import VerbosityMode, init, get_mode, get_console, get_stderr_console
import ydb.tools.mnc.lib.output as output_module


class OutputTest(unittest.TestCase):
    def setUp(self):
        """Reset output module state before each test."""
        output_module._state = {
            "mode": VerbosityMode.NORMAL,
            "console": None,
            "stderr_console": None,
        }

    def test_default_mode_is_normal(self):
        init()
        self.assertEqual(get_mode(), VerbosityMode.NORMAL)

    def test_mode_switching(self):
        # Test that we can set different modes (each test starts fresh due to setUp)
        init(VerbosityMode.QUIET)
        self.assertEqual(get_mode(), VerbosityMode.QUIET)

        # Reset and try different mode
        output_module._state = {
            "mode": VerbosityMode.NORMAL,
            "console": None,
            "stderr_console": None,
        }
        init(VerbosityMode.VERBOSE)
        self.assertEqual(get_mode(), VerbosityMode.VERBOSE)

        # Reset and try normal mode
        output_module._state = {
            "mode": VerbosityMode.NORMAL,
            "console": None,
            "stderr_console": None,
        }
        init(VerbosityMode.NORMAL)
        self.assertEqual(get_mode(), VerbosityMode.NORMAL)

    def test_init_is_idempotent(self):
        """Test that multiple init calls with same mode don't create new consoles."""
        init(VerbosityMode.NORMAL)
        console1 = get_console()
        stderr_console1 = get_stderr_console()

        # Call init again with same mode
        init(VerbosityMode.NORMAL)
        console2 = get_console()
        stderr_console2 = get_stderr_console()

        # Should be the same instances
        self.assertIs(console1, console2)
        self.assertIs(stderr_console1, stderr_console2)

    def test_get_console_raises_if_not_initialized(self):
        """Test that get_console raises RuntimeError if init not called."""
        with self.assertRaises(RuntimeError) as context:
            get_console()
        self.assertIn("init() must be called", str(context.exception))

    def test_get_stderr_console_raises_if_not_initialized(self):
        """Test that get_stderr_console raises RuntimeError if init not called."""
        with self.assertRaises(RuntimeError) as context:
            get_stderr_console()
        self.assertIn("init() must be called", str(context.exception))

    def test_get_console_returns_rich_console(self):
        """Test that get_console returns a Rich Console instance."""
        init()
        console = get_console()
        import rich.console
        self.assertIsInstance(console, rich.console.Console)

    def test_get_stderr_console_returns_rich_console(self):
        """Test that get_stderr_console returns a Rich Console instance."""
        init()
        console = get_stderr_console()
        import rich.console
        self.assertIsInstance(console, rich.console.Console)

    def test_stderr_console_is_for_stderr(self):
        """Test that stderr console is configured for stderr output."""
        init()
        console = get_stderr_console()
        self.assertTrue(console.stderr)


if __name__ == '__main__':
    unittest.main()
