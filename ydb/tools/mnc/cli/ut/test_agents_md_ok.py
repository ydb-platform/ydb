import unittest

from ydb.tools.mnc.lib.agents_md_ok import describe_probe
from ydb.tools.mnc.lib.exceptions import CliError
from ydb.tools.mnc.lib.output import VerbosityMode
from ydb.tools.mnc.lib.progress import TaskResultLevel
import ydb.tools.mnc.lib.output as output_module


class AgentsMdOkTest(unittest.TestCase):
    def setUp(self):
        output_module._state = {
            "mode": VerbosityMode.NORMAL,
            "console": None,
            "stderr_console": None,
            "active_progress": None,
            "progress_backend_override": None,
        }
        output_module.init()

    def test_empty_name_raises_cli_error(self):
        with self.assertRaises(CliError):
            describe_probe("")

    def test_success_returns_task_result(self):
        result = describe_probe("ok")
        self.assertTrue(result)
        self.assertEqual(result.level, TaskResultLevel.OK)
        self.assertEqual(result.message, "ok")
