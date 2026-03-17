import os
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest import TestCase

from tap import Tap


# Suppress prints from SystemExit
class DevNull:
    def write(self, msg):
        pass


sys.stderr = DevNull()


class LoadConfigFilesTests(TestCase):
    def test_file_does_not_exist(self) -> None:
        class EmptyTap(Tap):
            pass

        with self.assertRaises(FileNotFoundError):
            EmptyTap(config_files=["nope"]).parse_args([])

    def test_single_config(self) -> None:
        class SimpleTap(Tap):
            a: int
            b: str = "b"

        with TemporaryDirectory() as temp_dir:
            fname = os.path.join(temp_dir, "config.txt")

            with open(fname, "w") as f:
                f.write("--a 1")

            args = SimpleTap(config_files=[fname]).parse_args([])

        self.assertEqual(args.a, 1)
        self.assertEqual(args.b, "b")

    def test_single_config_overwriting(self) -> None:
        class SimpleOverwritingTap(Tap):
            a: int
            b: str = "b"

        with TemporaryDirectory() as temp_dir:
            fname = os.path.join(temp_dir, "config.txt")

            with open(fname, "w") as f:
                f.write("--a 1 --b two")

            args = SimpleOverwritingTap(config_files=[fname]).parse_args("--a 2".split())

        self.assertEqual(args.a, 2)
        self.assertEqual(args.b, "two")

    def test_single_config_known_only(self) -> None:
        class KnownOnlyTap(Tap):
            a: int
            b: str = "b"

        with TemporaryDirectory() as temp_dir:
            fname = os.path.join(temp_dir, "config.txt")

            with open(fname, "w") as f:
                f.write("--a 1 --c seeNothing")

            args = KnownOnlyTap(config_files=[fname]).parse_args([], known_only=True)

        self.assertEqual(args.a, 1)
        self.assertEqual(args.b, "b")
        self.assertEqual(args.extra_args, ["--c", "seeNothing"])

    def test_single_config_required_still_required(self) -> None:
        class KnownOnlyTap(Tap):
            a: int
            b: str = "b"

        with TemporaryDirectory() as temp_dir, self.assertRaises(SystemExit):
            fname = os.path.join(temp_dir, "config.txt")

            with open(fname, "w") as f:
                f.write("--b fore")

            KnownOnlyTap(config_files=[fname]).parse_args([])

    def test_multiple_configs(self) -> None:
        class MultipleTap(Tap):
            a: int
            b: str = "b"

        with TemporaryDirectory() as temp_dir:
            fname1, fname2 = os.path.join(temp_dir, "config1.txt"), os.path.join(temp_dir, "config2.txt")

            with open(fname1, "w") as f1, open(fname2, "w") as f2:
                f1.write("--b two")
                f2.write("--a 1")

            args = MultipleTap(config_files=[fname1, fname2]).parse_args([])

        self.assertEqual(args.a, 1)
        self.assertEqual(args.b, "two")

    def test_multiple_configs_overwriting(self) -> None:
        class MultipleOverwritingTap(Tap):
            a: int
            b: str = "b"
            c: str = "c"

        with TemporaryDirectory() as temp_dir:
            fname1, fname2 = os.path.join(temp_dir, "config1.txt"), os.path.join(temp_dir, "config2.txt")

            with open(fname1, "w") as f1, open(fname2, "w") as f2:
                f1.write("--a 1 --b two")
                f2.write("--a 2 --c see")

            args = MultipleOverwritingTap(config_files=[fname1, fname2]).parse_args("--b four".split())

        self.assertEqual(args.a, 2)
        self.assertEqual(args.b, "four")
        self.assertEqual(args.c, "see")

    def test_junk_config(self) -> None:
        class JunkConfigTap(Tap):
            a: int
            b: str = "b"

        with TemporaryDirectory() as temp_dir, self.assertRaises(SystemExit):
            fname = os.path.join(temp_dir, "config.txt")

            with open(fname, "w") as f:
                f.write("is not a file that can reasonably be parsed")

            JunkConfigTap(config_files=[fname]).parse_args([])

    def test_shlex_config(self) -> None:
        class ShlexConfigTap(Tap):
            a: int
            b: str

        with TemporaryDirectory() as temp_dir:
            fname = os.path.join(temp_dir, "config.txt")

            with open(fname, "w") as f:
                f.write('--a 21 # Important arg value\n\n# Multi-word quoted string\n--b "two three four"')

            args = ShlexConfigTap(config_files=[fname]).parse_args([])

        self.assertEqual(args.a, 21)
        self.assertEqual(args.b, "two three four")


if __name__ == "__main__":
    unittest.main()
