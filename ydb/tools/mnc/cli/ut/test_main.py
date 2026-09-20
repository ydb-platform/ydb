import os
import tempfile
import types
import unittest
from unittest import mock

from ydb.tools.mnc.cli import command_options
from ydb.tools.mnc.cli import main
from ydb.tools.mnc.cli import parser_factory
from ydb.tools.mnc.lib import progress
from ydb.tools.mnc.lib.exceptions import CliError
from ydb.tools.mnc.lib.output import VerbosityMode


def _add_work_directory(parser):
    parser.add_argument('--wd', '--work-directory', dest='work_directory', default=None)


def _patch_parser(module):
    parser, actions, expected_config, prefer_launcher = parser_factory.build_parser([module])
    return mock.patch.object(
        main.parser_factory,
        "build_parser",
        return_value=(parser, actions, expected_config, prefer_launcher),
    )


class CliMainTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        """Reset output module state before each test."""
        import ydb.tools.mnc.lib.output as output_module
        output_module._state = {
            "mode": VerbosityMode.NORMAL,
            "console": None,
            "stderr_console": None,
        }

    async def test_async_main_raises_cli_error_when_command_returns_false(self):
        async def do(args):
            return False

        module = types.SimpleNamespace(
            __name__="fail",
            expected_config=None,
            add_arguments=lambda parser: None,
            do=do,
        )

        with _patch_parser(module), mock.patch("sys.argv", ["mnc", "fail"]):
            with self.assertRaises(CliError) as error:
                await main.async_main()

        self.assertEqual(str(error.exception), "Command 'fail' failed")

    async def test_async_main_attaches_failed_task_result_to_cli_error(self):
        command_result = progress.TaskResult(
            level=progress.TaskResultLevel.ERROR,
            step_title="failed-step",
            message="actionable failure",
        )

        async def do(args):
            return command_result

        module = types.SimpleNamespace(
            __name__="fail_result",
            expected_config=None,
            add_arguments=lambda parser: None,
            do=do,
        )

        with _patch_parser(module), mock.patch("sys.argv", ["mnc", "fail_result"]):
            with self.assertRaises(CliError) as error:
                await main.async_main()

        self.assertEqual(str(error.exception), "Command 'fail_result' failed")
        self.assertIs(error.exception.result, command_result)

    async def test_mutually_exclusive_verbose_and_quiet(self):
        """Test that -V and -q cannot be used together."""
        async def do(args):
            return True

        module = types.SimpleNamespace(
            __name__="test",
            expected_config=None,
            add_arguments=lambda parser: None,
            do=do,
        )

        with _patch_parser(module), mock.patch("sys.argv", ["mnc", "-V", "-q", "test"]):
            with self.assertRaises(SystemExit):
                await main.async_main()

    async def test_config_validation_raises_cli_error(self):
        """Test that config validation errors raise CliError instead of sys.exit."""
        async def do(args):
            return True

        module = types.SimpleNamespace(
            __name__="test",
            expected_config={"test": "scheme"},
            add_arguments=_add_work_directory,
            do=do,
        )

        # Mock scheme.apply_scheme to return validation errors
        with _patch_parser(module), \
             mock.patch("sys.argv", ["mnc", "test"]), \
             mock.patch.object(main.scheme, "apply_scheme") as mock_apply, \
             mock.patch.object(main.config, "get_config", return_value={}):

            mock_apply.return_value = (None, ["error1", "error2"])

            with self.assertRaises(CliError) as context:
                await main.async_main()

            error_msg = str(context.exception)
            self.assertIn("Config validation failed", error_msg)
            self.assertIn("error1", error_msg)
            self.assertIn("error2", error_msg)

    def test_global_exception_handler_shows_rich_traceback(self):
        """Test that unhandled exceptions show rich traceback via main()."""
        async def do(args):
            raise RuntimeError("boom")

        module = types.SimpleNamespace(
            __name__="boom",
            expected_config=None,
            add_arguments=lambda parser: None,
            do=do,
        )

        with _patch_parser(module), \
             mock.patch("sys.argv", ["mnc", "boom"]), \
             mock.patch.object(main.config, "get_mnc_config", return_value={}), \
             mock.patch.object(main.deploy_ctx, "apply_cfg_mnc"):
            with self.assertRaises(SystemExit) as context:
                main.main()

            # Should exit with code 1
            self.assertEqual(context.exception.code, 1)

    async def test_command_returning_none_raises_cli_error(self):
        """Test that commands returning None raise CliError."""
        async def do(args):
            return None

        module = types.SimpleNamespace(
            __name__="none",
            expected_config=None,
            add_arguments=lambda parser: None,
            do=do,
        )

        with _patch_parser(module), mock.patch("sys.argv", ["mnc", "none"]):
            with self.assertRaises(CliError) as error:
                await main.async_main()

        self.assertEqual(str(error.exception), "Command 'none' failed")

    async def test_command_returning_zero_raises_cli_error(self):
        """Test that commands returning 0 raise CliError."""
        async def do(args):
            return 0

        module = types.SimpleNamespace(
            __name__="zero",
            expected_config=None,
            add_arguments=lambda parser: None,
            do=do,
        )

        with _patch_parser(module), mock.patch("sys.argv", ["mnc", "zero"]):
            with self.assertRaises(CliError) as error:
                await main.async_main()

        self.assertEqual(str(error.exception), "Command 'zero' failed")

    async def test_command_returning_empty_list_raises_cli_error(self):
        """Test that commands returning [] raise CliError."""
        async def do(args):
            return []

        module = types.SimpleNamespace(
            __name__="empty",
            expected_config=None,
            add_arguments=lambda parser: None,
            do=do,
        )

        with _patch_parser(module), mock.patch("sys.argv", ["mnc", "empty"]):
            with self.assertRaises(CliError) as error:
                await main.async_main()

        self.assertEqual(str(error.exception), "Command 'empty' failed")

    async def test_async_main_does_not_apply_cached_options_without_tui(self):
        async def do(args):
            return True

        def add_arguments(parser):
            parser.add_argument("--host", required=True)

        module = types.SimpleNamespace(
            __name__="cached",
            expected_config=None,
            add_arguments=add_arguments,
            do=do,
        )

        with tempfile.TemporaryDirectory() as home:
            with mock.patch.dict(os.environ, {"HOME": home}):
                command_options.save_cache({"cached": {"tokens": ["--host", "cached-host"]}})

                with _patch_parser(module), mock.patch("sys.argv", ["mnc", "cached"]):
                    with self.assertRaises(SystemExit):
                        await main.async_main()

    async def test_async_main_applies_cached_options_for_tui(self):
        seen = {}

        async def do(args):
            seen["host"] = args.host
            return True

        async def run_tui_action(action):
            return await action(None)

        def add_arguments(parser):
            parser.add_argument("--host", required=True)

        module = types.SimpleNamespace(
            __name__="cached",
            expected_config=None,
            add_arguments=add_arguments,
            do=do,
        )

        with tempfile.TemporaryDirectory() as home:
            with mock.patch.dict(os.environ, {"HOME": home}):
                command_options.save_cache({"cached": {"tokens": ["--host", "cached-host"]}})

                with _patch_parser(module), \
                     mock.patch("sys.argv", ["mnc", "--tui", "cached"]), \
                     mock.patch.object(main.TuiApp, "run_async", side_effect=run_tui_action):
                    await main.async_main()

        self.assertEqual(seen["host"], "cached-host")
