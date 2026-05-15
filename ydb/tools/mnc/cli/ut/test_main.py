import types
import unittest
from unittest import mock

from ydb.tools.mnc.cli import main
from ydb.tools.mnc.lib.exceptions import CliError


class CliMainTest(unittest.IsolatedAsyncioTestCase):
    async def test_async_main_raises_cli_error_when_command_returns_false(self):
        async def do(args):
            return False

        module = types.SimpleNamespace(
            __name__="ydb.tools.mnc.cli.commands.fail",
            expected_config=None,
            add_arguments=lambda parser: None,
            do=do,
        )

        with mock.patch.object(main, "modules", [module]), mock.patch("sys.argv", ["mnc", "fail"]):
            with self.assertRaises(CliError) as error:
                await main.async_main()

        self.assertEqual(str(error.exception), "Command 'fail' failed")
