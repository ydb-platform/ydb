from argparse import ArgumentError
import sys
from typing import Literal, Union
import unittest
from unittest import TestCase

from tap import Tap


# Suppress prints from SystemExit
class DevNull:
    def write(self, msg):
        pass


sys.stderr = DevNull()


class TestSubparser(TestCase):
    def test_subparser_documentation_example(self):
        class SubparserA(Tap):
            bar: int  # bar help

        class SubparserB(Tap):
            baz: Literal["X", "Y", "Z"]  # baz help

        class Args(Tap):
            foo: bool = False  # foo help

            def configure(self):
                self.add_subparsers(help="sub-command help")
                self.add_subparser("a", SubparserA, help="a help")
                self.add_subparser("b", SubparserB, help="b help")

        args = Args().parse_args([])
        self.assertFalse(args.foo)
        self.assertFalse(hasattr(args, "bar"))
        self.assertFalse(hasattr(args, "baz"))

        args = Args().parse_args(["--foo"])
        self.assertTrue(args.foo)
        self.assertFalse(hasattr(args, "bar"))
        self.assertFalse(hasattr(args, "baz"))

        args = Args().parse_args("a --bar 1".split())
        self.assertFalse(args.foo)
        self.assertEqual(args.bar, 1)
        self.assertFalse(hasattr(args, "baz"))

        args = Args().parse_args("--foo b --baz X".split())
        self.assertTrue(args.foo)
        self.assertFalse(hasattr(args, "bar"))
        self.assertEqual(args.baz, "X")

        with self.assertRaises(SystemExit):
            Args().parse_args("--baz X --foo b".split())

        with self.assertRaises(SystemExit):
            Args().parse_args("b --baz X --foo".split())

        with self.assertRaises(SystemExit):
            Args().parse_args("--foo a --bar 1 b --baz X".split())

    def test_name_collision(self):
        class SubparserA(Tap):
            a: int

        class Args(Tap):
            foo: bool = False

            def configure(self):
                self.add_subparsers(help="sub-command help")
                self.add_subparser("a", SubparserA, help="a help")

        args = Args().parse_args("a --a 1".split())
        self.assertFalse(args.foo)
        self.assertEqual(args.a, 1)

    def test_name_overriding(self):
        class SubparserA(Tap):
            foo: int

        class Args(Tap):
            foo: bool = False

            def configure(self):
                self.add_subparsers(help="sub-command help")
                self.add_subparser("a", SubparserA)

        args = Args().parse_args(["--foo"])
        self.assertTrue(args.foo)

        args = Args().parse_args("a --foo 2".split())
        self.assertEqual(args.foo, 2)

        args = Args().parse_args("--foo a --foo 2".split())
        self.assertEqual(args.foo, 2)

    def test_add_subparser_twice(self):
        class SubparserA(Tap):
            bar: int

        class SubparserB(Tap):
            baz: int

        class Args(Tap):
            foo: bool = False

            def configure(self):
                self.add_subparser("a", SubparserB)
                self.add_subparser("a", SubparserA)

        if sys.version_info >= (3, 14):
            with self.assertRaises(ValueError, msg="conflicting subparser: a"):
                Args().parse_args([])
        elif sys.version_info >= (3, 11):
            with self.assertRaises(ArgumentError, msg="argument {a}: conflicting subparser: a"):
                Args().parse_args([])
        else:
            args = Args().parse_args("a --bar 2".split())
            self.assertFalse(args.foo)
            self.assertEqual(args.bar, 2)
            self.assertFalse(hasattr(args, "baz"))

            with self.assertRaises(SystemExit):
                Args().parse_args("a --baz 2".split())

    def test_add_subparsers_twice(self):
        class SubparserA(Tap):
            a: int

        class Args(Tap):
            foo: bool = False

            def configure(self):
                self.add_subparser("a", SubparserA)
                self.add_subparsers(help="sub-command1 help")
                self.add_subparsers(help="sub-command2 help")

        if sys.version_info >= (3, 14):
            with self.assertRaises(ValueError, msg="cannot have multiple subparser arguments"):
                Args().parse_args([])
        elif sys.version_info >= (3, 12, 5):
            with self.assertRaises(ArgumentError, msg="cannot have multiple subparser arguments"):
                Args().parse_args([])
        else:
            with self.assertRaises(SystemExit):
                Args().parse_args([])

    def test_add_subparsers_with_add_argument(self):
        class SubparserA(Tap):
            for_sure: bool = False

        class Args(Tap):
            foo: bool = False
            bar: int = 1

            def configure(self):
                self.add_argument("--bar", "-ib")
                self.add_subparser("is_terrible", SubparserA)
                self.add_argument("--foo", "-m")

        args = Args().parse_args("-ib 0 -m is_terrible --for_sure".split())
        self.assertTrue(args.foo)
        self.assertEqual(args.bar, 0)
        self.assertTrue(args.for_sure)

    def test_add_subsubparsers(self):
        class SubSubparserB(Tap):
            baz: bool = False

        class SubparserA(Tap):
            biz: bool = False

            def configure(self):
                self.add_subparser("b", SubSubparserB)

        class SubparserB(Tap):
            blaz: bool = False

        class Args(Tap):
            foo: bool = False

            def configure(self):
                self.add_subparser("a", SubparserA)
                self.add_subparser("b", SubparserB)

        args = Args().parse_args("b --blaz".split())
        self.assertFalse(args.foo)
        self.assertFalse(hasattr(args, "baz"))
        self.assertFalse(hasattr(args, "biz"))
        self.assertTrue(args.blaz)

        args = Args().parse_args("a --biz".split())
        self.assertFalse(args.foo)
        self.assertTrue(args.biz)
        self.assertFalse(hasattr(args, "baz"))
        self.assertFalse(hasattr(args, "blaz"))

        args = Args().parse_args("a --biz b --baz".split())
        self.assertFalse(args.foo)
        self.assertTrue(args.biz)
        self.assertFalse(hasattr(args, "blaz"))
        self.assertTrue(args.baz)

        with self.assertRaises(SystemExit):
            Args().parse_args("b a".split())

    def test_subparser_underscores_to_dashes(self):
        class AddProposal(Tap):
            proposal_id: int

        class Arguments(Tap):
            def configure(self) -> None:
                self.add_subparsers(dest="subparser_name")

                self.add_subparser(
                    "add-proposal", AddProposal, help="Add a new proposal",
                )

        args_underscores: Union[Arguments, AddProposal] = Arguments(underscores_to_dashes=False).parse_args(
            "add-proposal --proposal_id 1".split()
        )
        self.assertEqual(args_underscores.proposal_id, 1)

        args_dashes: Union[Arguments, AddProposal] = Arguments(underscores_to_dashes=True).parse_args(
            "add-proposal --proposal-id 1".split()
        )
        self.assertEqual(args_dashes.proposal_id, 1)

        with self.assertRaises(SystemExit):
            Arguments(underscores_to_dashes=False).parse_args("add-proposal --proposal-id 1".split())

        with self.assertRaises(SystemExit):
            Arguments(underscores_to_dashes=True).parse_args("add-proposal --proposal_id 1".split())


if __name__ == "__main__":
    unittest.main()
