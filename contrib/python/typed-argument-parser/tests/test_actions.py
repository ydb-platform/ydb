from typing import List, Literal
import unittest
from unittest import TestCase

from tap import Tap


class TestArgparseActions(TestCase):
    def test_actions_store_const(self):
        class StoreConstTap(Tap):
            def configure(self):
                self.add_argument("--sum", dest="accumulate", action="store_const", const=sum, default=max)

        args = StoreConstTap().parse_args([])
        self.assertFalse(hasattr(args, "sum"))
        self.assertEqual(args.accumulate, max)
        self.assertEqual(args.as_dict(), {"accumulate": max})

        args = StoreConstTap().parse_args(["--sum"])
        self.assertFalse(hasattr(args, "sum"))
        self.assertEqual(args.accumulate, sum)
        self.assertEqual(args.as_dict(), {"accumulate": sum})

    def test_actions_store_true_default_true(self):
        class StoreTrueDefaultTrueTap(Tap):
            foobar: bool = True

            def configure(self):
                self.add_argument("--foobar", action="store_true")

        args = StoreTrueDefaultTrueTap().parse_args([])
        self.assertTrue(args.foobar)

        args = StoreTrueDefaultTrueTap().parse_args(["--foobar"])
        self.assertTrue(args.foobar)

    def test_actions_store_true_default_false(self):
        class StoreTrueDefaultFalseTap(Tap):
            foobar: bool = False

            def configure(self):
                self.add_argument("--foobar", action="store_true")

        args = StoreTrueDefaultFalseTap().parse_args([])
        self.assertFalse(args.foobar)

        args = StoreTrueDefaultFalseTap().parse_args(["--foobar"])
        self.assertTrue(args.foobar)

    def test_actions_store_false_default_true(self):
        class StoreFalseDefaultTrueTap(Tap):
            foobar: bool = True

            def configure(self):
                self.add_argument("--foobar", action="store_false")

        args = StoreFalseDefaultTrueTap().parse_args([])
        self.assertTrue(args.foobar)

        args = StoreFalseDefaultTrueTap().parse_args(["--foobar"])
        self.assertFalse(args.foobar)

    def test_actions_store_false_default_false(self):
        class StoreFalseDefaultFalseTap(Tap):
            foobar: bool = False

            def configure(self):
                self.add_argument("--foobar", action="store_false")

        args = StoreFalseDefaultFalseTap().parse_args([])
        self.assertFalse(args.foobar)

        args = StoreFalseDefaultFalseTap().parse_args(["--foobar"])
        self.assertFalse(args.foobar)

    def test_actions_append_list(self):
        class AppendListTap(Tap):
            arg: List = ["what", "is"]

            def configure(self):
                self.add_argument("--arg", action="append")

        args = AppendListTap().parse_args([])
        self.assertEqual(args.arg, ["what", "is"])

        args = AppendListTap().parse_args("--arg up --arg today".split())
        self.assertEqual(args.arg, "what is up today".split())

    def test_actions_append_list_int(self):
        class AppendListIntTap(Tap):
            arg: List[int] = [1, 2]

            def configure(self):
                self.add_argument("--arg", action="append")

        args = AppendListIntTap().parse_args("--arg 3 --arg 4".split())
        self.assertEqual(args.arg, [1, 2, 3, 4])

    def test_actions_append_list_literal(self):
        class AppendListLiteralTap(Tap):
            arg: List[Literal["what", "is", "up", "today"]] = ["what", "is"]

            def configure(self):
                self.add_argument("--arg", action="append")

        args = AppendListLiteralTap().parse_args("--arg up --arg today".split())
        self.assertEqual(args.arg, "what is up today".split())

    def test_actions_append_untyped(self):
        class AppendListStrTap(Tap):
            arg = ["what", "is"]

            def configure(self):
                self.add_argument("--arg", action="append")

        args = AppendListStrTap().parse_args([])
        self.assertEqual(args.arg, ["what", "is"])

        args = AppendListStrTap().parse_args("--arg up --arg today".split())
        self.assertEqual(args.arg, "what is up today".split())

    def test_actions_append_const(self):
        class AppendConstTap(Tap):
            arg: List[int] = [1, 2, 3]

            def configure(self):
                self.add_argument("--arg", action="append_const", const=7)

        args = AppendConstTap().parse_args([])
        self.assertEqual(args.arg, [1, 2, 3])

        args = AppendConstTap().parse_args("--arg --arg".split())
        self.assertEqual(args.arg, [1, 2, 3, 7, 7])

    def test_actions_count(self):
        class CountTap(Tap):
            arg = 7

            def configure(self):
                self.add_argument("--arg", "-a", action="count")

        args = CountTap().parse_args([])
        self.assertEqual(args.arg, 7)

        args = CountTap().parse_args("-aaa --arg".split())
        self.assertEqual(args.arg, 11)

    def test_actions_int_count(self):
        class CountIntTap(Tap):
            arg: int = 7

            def configure(self):
                self.add_argument("--arg", "-a", action="count")

        args = CountIntTap().parse_args([])
        self.assertEqual(args.arg, 7)

        args = CountIntTap().parse_args("-aaa --arg".split())
        self.assertEqual(args.arg, 11)

    def test_actions_version(self):
        class VersionTap(Tap):
            def configure(self):
                self.add_argument("--version", action="version", version="2.0")

        # Ensure that nothing breaks without version flag
        VersionTap().parse_args([])

        # TODO: With version flag testing fails, but manual tests work
        # tried redirecting stderr using unittest.mock.patch
        # VersionTap().parse_args(['--version'])

    def test_actions_extend(self):
        class ExtendTap(Tap):
            arg = [1, 2]

            def configure(self):
                self.add_argument("--arg", nargs="+", action="extend")

        args = ExtendTap().parse_args([])
        self.assertEqual(args.arg, [1, 2])

        args = ExtendTap().parse_args("--arg a b --arg a --arg c d".split())
        self.assertEqual(args.arg, [1, 2] + "a b a c d".split())

    def test_actions_extend_list(self):
        class ExtendListTap(Tap):
            arg: List = ["hi"]

            def configure(self):
                self.add_argument("--arg", action="extend")

        args = ExtendListTap().parse_args("--arg yo yo --arg yoyo --arg yo yo".split())
        self.assertEqual(args.arg, "hi yo yo yoyo yo yo".split())

    def test_actions_extend_list_int(self):
        class ExtendListIntTap(Tap):
            arg: List[int] = [0]

            def configure(self):
                self.add_argument("--arg", action="extend")

        args = ExtendListIntTap().parse_args("--arg 1 2 --arg 3 --arg 4 5".split())
        self.assertEqual(args.arg, [0, 1, 2, 3, 4, 5])

    def test_positional_default(self):
        class PositionalDefault(Tap):
            arg: str

            def configure(self):
                self.add_argument("arg")

        help_regex = r".*positional arguments:\n.*arg\s*\(str, required\).*"
        help_text = PositionalDefault().format_help()
        self.assertRegex(help_text, help_regex)


if __name__ == "__main__":
    unittest.main()
