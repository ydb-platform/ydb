import unittest

from ydb.tools.mnc.lib.agents_md_bad import describe_probe


class AgentsMdBadTest(unittest.TestCase):
    def test_empty_name_exits(self):
        with self.assertRaises(SystemExit) as ctx:
            describe_probe("")
        self.assertEqual(ctx.exception.code, 1)

    def test_success_returns_false(self):
        self.assertFalse(describe_probe("bad"))
