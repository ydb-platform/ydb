from argparse import ArgumentTypeError
from copy import deepcopy
import os
from pathlib import Path
import pickle
import sys
from tempfile import TemporaryDirectory
from typing import Any, Iterable, List, Literal, Optional, Set, Tuple, Union
import unittest
from unittest import TestCase

from tap import Tap


# Suppress prints from SystemExit
class DevNull:
    def write(self, msg):
        pass


sys.stderr = DevNull()


def stringify(arg_list: Iterable[Any]) -> list[str]:
    """Converts an iterable of arguments of any type to a list of strings.

    :param arg_list: An iterable of arguments of any type.
    :return: A list of the arguments as strings.
    """
    return [str(arg) for arg in arg_list]


class EdgeCaseTests(TestCase):
    @staticmethod
    def test_empty() -> None:
        class EmptyTap(Tap):
            pass

        EmptyTap().parse_args([])

    def test_empty_add_argument(self) -> None:
        class EmptyAddArgument(Tap):
            def configure(self) -> None:
                self.add_argument("--hi")

        hi = "yo"
        args = EmptyAddArgument().parse_args(["--hi", hi])
        self.assertEqual(args.hi, hi)

    def test_no_typed_args(self) -> None:
        class NoTypedTap(Tap):
            hi = 3

        args = NoTypedTap().parse_args([])
        self.assertEqual(args.hi, 3)

        hi = "yo"
        args = NoTypedTap().parse_args(["--hi", hi])
        self.assertEqual(args.hi, hi)

    def test_only_typed_args(self) -> None:
        class OnlyTypedTap(Tap):
            hi: str = "sup"

        args = OnlyTypedTap().parse_args([])
        self.assertEqual(args.hi, "sup")

        hi = "yo"
        args = OnlyTypedTap().parse_args(["--hi", hi])
        self.assertEqual(args.hi, hi)

    def test_type_as_string(self) -> None:
        class TypeAsString(Tap):
            a_number: "int" = 3
            a_list: "List[float]" = [3.7, 0.3]

        args = TypeAsString().parse_args([])
        self.assertEqual(args.a_number, 3)
        self.assertEqual(args.a_list, [3.7, 0.3])

        a_number = 42
        a_list = [3, 4, 0.7]

        args = TypeAsString().parse_args(["--a_number", str(a_number), "--a_list"] + [str(i) for i in a_list])
        self.assertEqual(args.a_number, a_number)
        self.assertEqual(args.a_list, a_list)

    def test_parse_twice(self) -> None:
        class ParseTwiceTap(Tap):
            a: int = 5

        with self.assertRaises(ValueError):
            ParseTwiceTap().parse_args(["--a", "6"]).parse_args(["--a", "7"])


class RequiredClassVariableTests(TestCase):
    def setUp(self) -> None:
        class RequiredArgumentsParser(Tap):
            arg_str_required: str
            arg_list_str_required: List[str]

        self.tap = RequiredArgumentsParser()

    def test_arg_str_required(self):
        with self.assertRaises(SystemExit):
            self.tap.parse_args(
                ["--arg_str_required", "tappy",]
            )

    def test_arg_list_str_required(self):
        with self.assertRaises(SystemExit):
            self.tap.parse_args(
                ["--arg_list_str_required", "hi", "there",]
            )

    def test_both_assigned_okay(self):
        args = self.tap.parse_args(["--arg_str_required", "tappy", "--arg_list_str_required", "hi", "there",])
        self.assertEqual(args.arg_str_required, "tappy")
        self.assertEqual(args.arg_list_str_required, ["hi", "there"])


class ParameterizedStandardCollectionTests(TestCase):
    def test_parameterized_standard_collection(self):
        class ParameterizedStandardCollectionTap(Tap):
            arg_list_str: list[str]
            arg_list_int: list[int]
            arg_list_int_default: list[int] = [1, 2, 5]
            arg_set_float: set[float]
            arg_set_str_default: set[str] = {"one", "two", "five"}
            arg_tuple_int: tuple[int, ...]
            arg_tuple_float_default: tuple[float, float, float] = (1.0, 2.0, 5.0)
            arg_tuple_str_override: tuple[str, str] = ("hi", "there")
            arg_optional_list_int: Optional[list[int]] = None

        arg_list_str = ["a", "b", "pi"]
        arg_list_int = [-2, -5, 10]
        arg_set_float = {3.54, 2.235}
        arg_tuple_int = (-4, 5, 9, 103)
        arg_tuple_str_override = ("why", "so")
        arg_optional_list_int = [5, 4, 3]

        args = ParameterizedStandardCollectionTap().parse_args(
            [
                "--arg_list_str",
                *arg_list_str,
                "--arg_list_int",
                *[str(var) for var in arg_list_int],
                "--arg_set_float",
                *[str(var) for var in arg_set_float],
                "--arg_tuple_int",
                *[str(var) for var in arg_tuple_int],
                "--arg_tuple_str_override",
                *arg_tuple_str_override,
                "--arg_optional_list_int",
                *[str(var) for var in arg_optional_list_int],
            ]
        )

        self.assertEqual(args.arg_list_str, arg_list_str)
        self.assertEqual(args.arg_list_int, arg_list_int)
        self.assertEqual(args.arg_list_int_default, ParameterizedStandardCollectionTap.arg_list_int_default)
        self.assertEqual(args.arg_set_float, arg_set_float)
        self.assertEqual(args.arg_set_str_default, ParameterizedStandardCollectionTap.arg_set_str_default)
        self.assertEqual(args.arg_tuple_int, arg_tuple_int)
        self.assertEqual(args.arg_tuple_float_default, ParameterizedStandardCollectionTap.arg_tuple_float_default)
        self.assertEqual(args.arg_tuple_str_override, arg_tuple_str_override)
        self.assertEqual(args.arg_optional_list_int, arg_optional_list_int)


class NestedOptionalTypesTap(Tap):
    list: Optional[List]
    list_bool: Optional[List[bool]]
    list_int: Optional[List[int]]
    list_str: Optional[List[str]]
    set: Optional[Set]
    set_bool: Optional[Set[bool]]
    set_int: Optional[Set[int]]
    set_str: Optional[Set[str]]
    tuple: Optional[Tuple]
    tuple_bool: Optional[Tuple[bool]]
    tuple_int: Optional[Tuple[int]]
    tuple_str: Optional[Tuple[str]]
    tuple_pair: Optional[Tuple[bool, str, int]]
    tuple_arbitrary_len_bool: Optional[Tuple[bool, ...]]
    tuple_arbitrary_len_int: Optional[Tuple[int, ...]]
    tuple_arbitrary_len_str: Optional[Tuple[str, ...]]


class NestedOptionalTypeTests(TestCase):
    def test_nested_optional_types(self):
        list_ = ["I", "was", "wondering"]
        list_bool = [True, False]
        list_int = [0, 1, 2]
        list_str = ["a", "bee", "cd", "ee"]
        set_ = {"if", "after", "all"}
        set_bool = {True, False, True}
        set_int = {0, 1}
        set_str = {"a", "bee", "cd"}
        tuple_ = ("these", "years")
        tuple_bool = (False,)
        tuple_int = (0,)
        tuple_str = ("a",)
        tuple_pair = (False, "a", 1)
        tuple_arbitrary_len_bool = (True, False, False)
        tuple_arbitrary_len_int = (1, 2, 3, 4)
        tuple_arbitrary_len_str = ("a", "b")

        args = NestedOptionalTypesTap().parse_args(
            [
                "--list",
                *stringify(list_),
                "--list_bool",
                *stringify(list_bool),
                "--list_int",
                *stringify(list_int),
                "--list_str",
                *stringify(list_str),
                "--set",
                *stringify(set_),
                "--set_bool",
                *stringify(set_bool),
                "--set_int",
                *stringify(set_int),
                "--set_str",
                *stringify(set_str),
                "--tuple",
                *stringify(tuple_),
                "--tuple_bool",
                *stringify(tuple_bool),
                "--tuple_int",
                *stringify(tuple_int),
                "--tuple_str",
                *stringify(tuple_str),
                "--tuple_pair",
                *stringify(tuple_pair),
                "--tuple_arbitrary_len_bool",
                *stringify(tuple_arbitrary_len_bool),
                "--tuple_arbitrary_len_int",
                *stringify(tuple_arbitrary_len_int),
                "--tuple_arbitrary_len_str",
                *stringify(tuple_arbitrary_len_str),
            ]
        )

        self.assertEqual(args.list, list_)
        self.assertEqual(args.list_bool, list_bool)
        self.assertEqual(args.list_int, list_int)
        self.assertEqual(args.list_str, list_str)

        self.assertEqual(args.set, set_)
        self.assertEqual(args.set_bool, set_bool)
        self.assertEqual(args.set_int, set_int)
        self.assertEqual(args.set_str, set_str)

        self.assertEqual(args.tuple, tuple_)
        self.assertEqual(args.tuple_bool, tuple_bool)
        self.assertEqual(args.tuple_int, tuple_int)
        self.assertEqual(args.tuple_str, tuple_str)
        self.assertEqual(args.tuple_pair, tuple_pair)
        self.assertEqual(args.tuple_arbitrary_len_bool, tuple_arbitrary_len_bool)
        self.assertEqual(args.tuple_arbitrary_len_int, tuple_arbitrary_len_int)
        self.assertEqual(args.tuple_arbitrary_len_str, tuple_arbitrary_len_str)


class ComplexTypeTap(Tap):
    path: Path
    optional_path: Optional[Path]
    list_path: List[Path]
    set_path: Set[Path]
    tuple_path: Tuple[Path, Path]


class ComplexTypeTests(TestCase):
    def test_complex_types(self):
        path = Path("/path/to/file.txt")
        optional_path = Path("/path/to/optional/file.txt")
        list_path = [Path("/path/to/list/file1.txt"), Path("/path/to/list/file2.txt")]
        set_path = {Path("/path/to/set/file1.txt"), Path("/path/to/set/file2.txt")}
        tuple_path = (Path("/path/to/tuple/file1.txt"), Path("/path/to/tuple/file2.txt"))

        args = ComplexTypeTap().parse_args(
            [
                "--path",
                str(path),
                "--optional_path",
                str(optional_path),
                "--list_path",
                *[str(path) for path in list_path],
                "--set_path",
                *[str(path) for path in set_path],
                "--tuple_path",
                *[str(path) for path in tuple_path],
            ]
        )

        self.assertEqual(args.path, path)
        self.assertEqual(args.optional_path, optional_path)
        self.assertEqual(args.list_path, list_path)
        self.assertEqual(args.set_path, set_path)
        self.assertEqual(args.tuple_path, tuple_path)


class Person:
    def __init__(self, name: str):
        self.name = name

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Person) and self.name == other.name


class IntegrationDefaultTap(Tap):
    """Documentation is boring"""

    arg_untyped = 42
    arg_str: str = "hello there"
    arg_int: int = -100
    arg_float: float = 77.3
    # TODO: how to handle untyped arguments?
    # users might accidentally think they should behave according to the inferred type
    # arg_bool_untyped_true = True
    # arg_bool_untyped_false = False
    arg_bool_true: bool = True
    arg_bool_false: bool = False
    arg_literal: Literal["english", "A", True, 88.9, 100] = "A"

    arg_optional: Optional = None
    arg_optional_str: Optional[str] = None
    arg_optional_int: Optional[int] = None
    arg_optional_float: Optional[float] = None
    arg_optional_bool: Optional[bool] = None
    arg_optional_literal: Optional[Literal["english", "A", True, 88.9, 100]] = None

    arg_list: List = ["these", "are", "strings"]
    arg_list_str: List[str] = ["hello", "how are you"]
    arg_list_int: List[int] = [10, -11]
    arg_list_float: List[float] = [3.14, 6.28]
    arg_list_bool: List[bool] = [True, False]
    arg_list_literal: List[Literal["H", 1, 1.00784, False]] = ["H", False]
    arg_list_str_empty: List[str] = []

    arg_set: Set = {"these", "are", "strings"}
    arg_set_str: Set[str] = {"hello", "how are you"}
    arg_set_int: Set[int] = {10, -11}
    arg_set_float: Set[float] = {3.14, 6.28}
    arg_set_bool: Set[bool] = {True, False}
    arg_set_literal: Set[Literal["H", 1, 1.00784, False]] = {"H", False}
    arg_set_str_empty: Set[str] = set()

    arg_tuple: Tuple = ("these", "are", "strings")
    arg_tuple_str: Tuple[str, ...] = ("hello", "how are you")
    arg_tuple_int: Tuple[int, ...] = (10, -11)
    arg_tuple_float: Tuple[float, ...] = (3.14, 6.28)
    arg_tuple_bool: Tuple[bool, ...] = (True, True, True, False)
    arg_tuple_multi: Tuple[float, int, str, bool, float] = (1.2, 1, "hi", True, 1.3)
    # TODO: move these elsewhere since we don't support them as defaults
    # arg_other_type_required: Person
    # arg_other_type_default: Person = Person('tap')


class SubclassTests(TestCase):
    def test_subclass(self) -> None:
        class IntegrationSubclassTap(IntegrationDefaultTap):
            arg_subclass_untyped = 33
            arg_subclass_str: str = "hello"
            arg_subclass_str_required: str
            arg_subclass_str_set_me: str = "goodbye"
            arg_float: float = -2.7

        arg_subclass_str_required = "subclassing is fun"
        arg_subclass_str_set_me = "all set!"
        arg_int = "77"
        self.args = IntegrationSubclassTap().parse_args(
            [
                "--arg_subclass_str_required",
                arg_subclass_str_required,
                "--arg_subclass_str_set_me",
                arg_subclass_str_set_me,
                "--arg_int",
                arg_int,
            ]
        )

        arg_int = int(arg_int)

        self.assertEqual(self.args.arg_str, IntegrationDefaultTap.arg_str)
        self.assertEqual(self.args.arg_int, arg_int)
        self.assertEqual(self.args.arg_float, -2.7)
        self.assertEqual(self.args.arg_subclass_str_required, arg_subclass_str_required)
        self.assertEqual(self.args.arg_subclass_str_set_me, arg_subclass_str_set_me)


class DefaultClassVariableTests(TestCase):
    def test_get_default_args(self) -> None:
        args = IntegrationDefaultTap().parse_args([])

        self.assertEqual(args.arg_untyped, 42)
        self.assertEqual(args.arg_str, "hello there")
        self.assertEqual(args.arg_int, -100)
        self.assertEqual(args.arg_float, 77.3)
        self.assertEqual(args.arg_bool_true, True)
        self.assertEqual(args.arg_bool_false, False)
        self.assertEqual(args.arg_literal, "A")

        self.assertTrue(args.arg_optional is None)
        self.assertTrue(args.arg_optional_str is None)
        self.assertTrue(args.arg_optional_int is None)
        self.assertTrue(args.arg_optional_float is None)
        self.assertTrue(args.arg_optional_bool is None)
        self.assertTrue(args.arg_optional_literal is None)

        self.assertEqual(args.arg_list, ["these", "are", "strings"])
        self.assertEqual(args.arg_list_str, ["hello", "how are you"])
        self.assertEqual(args.arg_list_int, [10, -11])
        self.assertEqual(args.arg_list_float, [3.14, 6.28])
        self.assertEqual(args.arg_list_bool, [True, False])
        self.assertEqual(args.arg_list_literal, ["H", False])
        self.assertEqual(args.arg_list_str_empty, [])

        self.assertEqual(args.arg_set, {"these", "are", "strings"})
        self.assertEqual(args.arg_set_str, {"hello", "how are you"})
        self.assertEqual(args.arg_set_int, {10, -11})
        self.assertEqual(args.arg_set_float, {3.14, 6.28})
        self.assertEqual(args.arg_set_bool, {True, False})
        self.assertEqual(args.arg_set_literal, {"H", False})
        self.assertEqual(args.arg_set_str_empty, set())

        self.assertEqual(args.arg_tuple, ("these", "are", "strings"))
        self.assertEqual(args.arg_tuple_str, ("hello", "how are you"))
        self.assertEqual(args.arg_tuple_int, (10, -11))
        self.assertEqual(args.arg_tuple_float, (3.14, 6.28))
        self.assertEqual(args.arg_tuple_bool, (True, True, True, False))
        self.assertEqual(args.arg_tuple_multi, (1.2, 1, "hi", True, 1.3))

    def test_set_default_args(self) -> None:
        arg_untyped = "yes"
        arg_str = "goodbye"
        arg_int = "2"
        arg_float = "1e-2"
        arg_literal = "True"

        arg_optional = "yo"
        arg_optional_str = "hello"
        arg_optional_int = "77"
        arg_optional_float = "7.7"
        arg_optional_bool = "tru"
        arg_optional_literal = "88.9"

        arg_list = ["string", "list"]
        arg_list_str = ["hi", "there", "how", "are", "you"]
        arg_list_int = ["1", "2", "3", "10", "-11"]
        arg_list_float = ["2.2", "-3.3", "2e20"]
        arg_list_bool = ["true", "F"]
        arg_list_str_empty = []
        arg_list_literal = ["H", "1"]

        arg_set = ["hi", "there"]
        arg_set_str = ["hi", "hi", "hi", "how"]
        arg_set_int = ["1", "2", "2", "2", "3"]
        arg_set_float = ["1.23", "4.4", "1.23"]
        arg_set_bool = ["tru", "fal"]
        arg_set_str_empty = []
        arg_set_literal = ["False", "1.00784"]

        arg_tuple = ("hi", "there")
        arg_tuple_str = ("hi", "hi", "hi", "how")
        arg_tuple_int = ("1", "2", "2", "2", "3")
        arg_tuple_float = ("1.23", "4.4", "1.23")
        arg_tuple_bool = ("tru", "fal")
        arg_tuple_multi = ("1.2", "1", "hi", "T", "1.3")

        args = IntegrationDefaultTap().parse_args(
            [
                "--arg_untyped",
                arg_untyped,
                "--arg_str",
                arg_str,
                "--arg_int",
                arg_int,
                "--arg_float",
                arg_float,
                "--arg_bool_true",
                "--arg_bool_false",
                "--arg_literal",
                arg_literal,
                "--arg_optional",
                arg_optional,
                "--arg_optional_str",
                arg_optional_str,
                "--arg_optional_int",
                arg_optional_int,
                "--arg_optional_float",
                arg_optional_float,
                "--arg_optional_bool",
                arg_optional_bool,
                "--arg_optional_literal",
                arg_optional_literal,
                "--arg_list",
                *arg_list,
                "--arg_list_str",
                *arg_list_str,
                "--arg_list_int",
                *arg_list_int,
                "--arg_list_float",
                *arg_list_float,
                "--arg_list_bool",
                *arg_list_bool,
                "--arg_list_str_empty",
                *arg_list_str_empty,
                "--arg_list_literal",
                *arg_list_literal,
                "--arg_set",
                *arg_set,
                "--arg_set_str",
                *arg_set_str,
                "--arg_set_int",
                *arg_set_int,
                "--arg_set_float",
                *arg_set_float,
                "--arg_set_bool",
                *arg_set_bool,
                "--arg_set_str_empty",
                *arg_set_str_empty,
                "--arg_set_literal",
                *arg_set_literal,
                "--arg_tuple",
                *arg_tuple,
                "--arg_tuple_str",
                *arg_tuple_str,
                "--arg_tuple_int",
                *arg_tuple_int,
                "--arg_tuple_float",
                *arg_tuple_float,
                "--arg_tuple_bool",
                *arg_tuple_bool,
                "--arg_tuple_multi",
                *arg_tuple_multi,
            ]
        )

        arg_int = int(arg_int)
        arg_float = float(arg_float)

        arg_optional_int = float(arg_optional_int)
        arg_optional_float = float(arg_optional_float)
        arg_optional_bool = True

        arg_list_int = [int(arg) for arg in arg_list_int]
        arg_list_float = [float(arg) for arg in arg_list_float]
        arg_list_bool = [True, False]

        arg_set = set(arg_set)
        arg_set_str = set(arg_set_str)
        arg_set_int = {int(arg) for arg in arg_set_int}
        arg_set_float = {float(arg) for arg in arg_set_float}
        arg_set_bool = {True, False}
        arg_set_str_empty = set()
        arg_set_literal = set(arg_set_literal)

        arg_tuple_int = tuple(int(arg) for arg in arg_tuple_int)
        arg_tuple_float = tuple(float(arg) for arg in arg_tuple_float)
        arg_tuple_bool = (True, False)
        arg_tuple_multi = (1.2, 1, "hi", True, 1.3)

        self.assertEqual(args.arg_untyped, arg_untyped)
        self.assertEqual(args.arg_str, arg_str)
        self.assertEqual(args.arg_int, arg_int)
        self.assertEqual(args.arg_float, arg_float)

        # Note: setting the bools as flags results in the opposite of their default
        self.assertEqual(args.arg_bool_true, False)
        self.assertEqual(args.arg_bool_false, True)
        self.assertEqual(args.arg_literal, True)

        self.assertEqual(args.arg_optional, arg_optional)
        self.assertEqual(args.arg_optional_str, arg_optional_str)
        self.assertEqual(args.arg_optional_int, arg_optional_int)
        self.assertEqual(args.arg_optional_float, arg_optional_float)
        self.assertEqual(args.arg_optional_bool, arg_optional_bool)
        self.assertEqual(args.arg_optional_literal, 88.9)

        self.assertEqual(args.arg_list, arg_list)
        self.assertEqual(args.arg_list_str, arg_list_str)
        self.assertEqual(args.arg_list_int, arg_list_int)
        self.assertEqual(args.arg_list_float, arg_list_float)
        self.assertEqual(args.arg_list_bool, arg_list_bool)
        self.assertEqual(args.arg_list_str_empty, arg_list_str_empty)
        self.assertEqual(args.arg_list_literal, ["H", 1])

        self.assertEqual(args.arg_set, arg_set)
        self.assertEqual(args.arg_set_str, arg_set_str)
        self.assertEqual(args.arg_set_int, arg_set_int)
        self.assertEqual(args.arg_set_float, arg_set_float)
        self.assertEqual(args.arg_set_bool, arg_set_bool)
        self.assertEqual(args.arg_set_str_empty, arg_set_str_empty)
        self.assertEqual(args.arg_set_literal, {False, 1.00784})

        self.assertEqual(args.arg_tuple, arg_tuple)
        self.assertEqual(args.arg_tuple_str, arg_tuple_str)
        self.assertEqual(args.arg_tuple_int, arg_tuple_int)
        self.assertEqual(args.arg_tuple_float, arg_tuple_float)
        self.assertEqual(args.arg_tuple_bool, arg_tuple_bool)
        self.assertEqual(args.arg_tuple_multi, arg_tuple_multi)


class LiteralCrashTests(TestCase):
    def test_literal_crash(self) -> None:
        class LiteralCrashTap(Tap):
            arg_lit: Literal["125", "no, 3 sir"]

        # Suppress prints from SystemExit
        class DevNull:
            def write(self, msg):
                pass

        with self.assertRaises(SystemExit):
            LiteralCrashTap().parse_args(["--arg_lit", "123"])


def convert_str_or_int(str_or_int: str) -> Union[str, int]:
    try:
        return int(str_or_int)
    except ValueError:
        return str_or_int


def convert_person_or_str(person_or_str: str) -> Union[Person, str]:
    if person_or_str == person_or_str.title():
        return Person(person_or_str)

    return person_or_str


def convert_many_types(input_str: str) -> Union[int, float, Person, str]:
    try:
        return int(input_str)
    except ValueError:
        try:
            return float(input_str)
        except ValueError:
            if input_str == input_str.title():
                return Person(input_str)

            return input_str


# TODO: test crash if not specifying type function
class UnionTypeTap(Tap):
    union_zero_required_arg: Union
    union_zero_default_arg: Union = "hi"
    union_one_required_arg: Union[str]
    union_one_default_arg: Union[str] = "there"
    union_two_required_arg: Union[str, int]
    union_two_default_int_arg: Union[str, int] = 5
    union_two_default_str_arg: Union[str, int] = "year old"
    union_custom_required_arg: Union[Person, str]
    union_custom_required_flip_arg: Union[str, Person]
    union_custom_default_arg: Union[Person, str] = Person("Jesse")
    union_custom_default_flip_arg: Union[str, Person] = "I want"
    union_none_required_arg: Union[int, None]
    union_none_required_flip_arg: Union[None, int]
    union_many_required_arg: Union[int, float, Person, str]
    union_many_default_arg: Union[int, float, Person, str] = 3.14

    def configure(self) -> None:
        self.add_argument("--union_two_required_arg", type=convert_str_or_int)
        self.add_argument("--union_two_default_int_arg", type=convert_str_or_int)
        self.add_argument("--union_two_default_str_arg", type=convert_str_or_int)
        self.add_argument("--union_custom_required_arg", type=convert_person_or_str)
        self.add_argument("--union_custom_required_flip_arg", type=convert_person_or_str)
        self.add_argument("--union_custom_default_arg", type=convert_person_or_str)
        self.add_argument("--union_custom_default_flip_arg", type=convert_person_or_str)
        self.add_argument("--union_none_required_flip_arg", type=int)
        self.add_argument("--union_many_required_arg", type=convert_many_types)
        self.add_argument("--union_many_default_arg", type=convert_many_types)


if sys.version_info >= (3, 10):

    class UnionType310Tap(Tap):
        union_two_required_arg: str | int
        union_two_default_int_arg: str | int = 10
        union_two_default_str_arg: str | int = "pieces of pie for"
        union_custom_required_arg: Person | str
        union_custom_required_flip_arg: str | Person
        union_custom_default_arg: Person | str = Person("Kyle")
        union_custom_default_flip_arg: str | Person = "making"
        union_none_required_arg: int | None
        union_none_required_flip_arg: None | int
        union_many_required_arg: int | float | Person | str
        union_many_default_arg: int | float | Person | str = 3.14 * 10 / 8

        def configure(self) -> None:
            self.add_argument("--union_two_required_arg", type=convert_str_or_int)
            self.add_argument("--union_two_default_int_arg", type=convert_str_or_int)
            self.add_argument("--union_two_default_str_arg", type=convert_str_or_int)
            self.add_argument("--union_custom_required_arg", type=convert_person_or_str)
            self.add_argument("--union_custom_required_flip_arg", type=convert_person_or_str)
            self.add_argument("--union_custom_default_arg", type=convert_person_or_str)
            self.add_argument("--union_custom_default_flip_arg", type=convert_person_or_str)
            self.add_argument("--union_none_required_flip_arg", type=int)
            self.add_argument("--union_many_required_arg", type=convert_many_types)
            self.add_argument("--union_many_default_arg", type=convert_many_types)


class UnionTypeTests(TestCase):
    def test_union_types(self):
        union_zero_required_arg = "Kyle"
        union_one_required_arg = "ate"
        union_two_required_arg = "2"
        union_custom_required_arg = "many"
        union_custom_required_flip_arg = "Jesse"
        union_none_required_arg = "1"
        union_none_required_flip_arg = "5"
        union_many_required_arg = "still hungry"

        args = UnionTypeTap().parse_args(
            [
                "--union_zero_required_arg",
                union_zero_required_arg,
                "--union_one_required_arg",
                union_one_required_arg,
                "--union_two_required_arg",
                union_two_required_arg,
                "--union_custom_required_arg",
                union_custom_required_arg,
                "--union_custom_required_flip_arg",
                union_custom_required_flip_arg,
                "--union_none_required_arg",
                union_none_required_arg,
                "--union_none_required_flip_arg",
                union_none_required_flip_arg,
                "--union_many_required_arg",
                union_many_required_arg,
            ]
        )

        union_two_required_arg = int(union_two_required_arg)
        union_custom_required_flip_arg = Person(union_custom_required_flip_arg)
        union_none_required_arg = int(union_none_required_arg)
        union_none_required_flip_arg = int(union_none_required_flip_arg)

        self.assertEqual(args.union_zero_required_arg, union_zero_required_arg)
        self.assertEqual(args.union_zero_default_arg, UnionTypeTap.union_zero_default_arg)
        self.assertEqual(args.union_one_required_arg, union_one_required_arg)
        self.assertEqual(args.union_one_default_arg, UnionTypeTap.union_one_default_arg)
        self.assertEqual(args.union_two_required_arg, union_two_required_arg)
        self.assertEqual(args.union_two_default_int_arg, UnionTypeTap.union_two_default_int_arg)
        self.assertEqual(args.union_two_default_str_arg, UnionTypeTap.union_two_default_str_arg)
        self.assertEqual(args.union_custom_required_arg, union_custom_required_arg)
        self.assertEqual(args.union_custom_required_flip_arg, union_custom_required_flip_arg)
        self.assertEqual(args.union_custom_default_arg, UnionTypeTap.union_custom_default_arg)
        self.assertEqual(args.union_custom_default_flip_arg, UnionTypeTap.union_custom_default_flip_arg)
        self.assertEqual(args.union_none_required_arg, union_none_required_arg)
        self.assertEqual(args.union_none_required_flip_arg, union_none_required_flip_arg)
        self.assertEqual(args.union_many_required_arg, union_many_required_arg)
        self.assertEqual(args.union_many_default_arg, UnionTypeTap.union_many_default_arg)

    def test_union_missing_type_function(self):
        class UnionMissingTypeFunctionTap(Tap):
            arg: Union[int, float]

        with self.assertRaises(ArgumentTypeError):
            UnionMissingTypeFunctionTap()

    @unittest.skipIf(sys.version_info < (3, 10), 'Union type operator "|" introduced in Python 3.10')
    def test_union_types_310(self):
        union_two_required_arg = "1"  # int
        union_custom_required_arg = "hungry"  # str
        union_custom_required_flip_arg = "Loser"  # Person
        union_none_required_arg = "8"  # int
        union_none_required_flip_arg = "100"  # int
        union_many_required_arg = "3.14"  # float

        args = UnionType310Tap().parse_args(
            [
                "--union_two_required_arg",
                union_two_required_arg,
                "--union_custom_required_arg",
                union_custom_required_arg,
                "--union_custom_required_flip_arg",
                union_custom_required_flip_arg,
                "--union_none_required_arg",
                union_none_required_arg,
                "--union_none_required_flip_arg",
                union_none_required_flip_arg,
                "--union_many_required_arg",
                union_many_required_arg,
            ]
        )

        union_two_required_arg = int(union_two_required_arg)
        union_custom_required_flip_arg = Person(union_custom_required_flip_arg)
        union_none_required_arg = int(union_none_required_arg)
        union_none_required_flip_arg = int(union_none_required_flip_arg)
        union_many_required_arg = float(union_many_required_arg)

        self.assertEqual(args.union_two_required_arg, union_two_required_arg)
        self.assertEqual(args.union_two_default_int_arg, UnionType310Tap.union_two_default_int_arg)
        self.assertEqual(args.union_two_default_str_arg, UnionType310Tap.union_two_default_str_arg)
        self.assertEqual(args.union_custom_required_arg, union_custom_required_arg)
        self.assertEqual(args.union_custom_required_flip_arg, union_custom_required_flip_arg)
        self.assertEqual(args.union_custom_default_arg, UnionType310Tap.union_custom_default_arg)
        self.assertEqual(args.union_custom_default_flip_arg, UnionType310Tap.union_custom_default_flip_arg)
        self.assertEqual(args.union_none_required_arg, union_none_required_arg)
        self.assertEqual(args.union_none_required_flip_arg, union_none_required_flip_arg)
        self.assertEqual(args.union_many_required_arg, union_many_required_arg)
        self.assertEqual(args.union_many_default_arg, UnionType310Tap.union_many_default_arg)

    @unittest.skipIf(sys.version_info < (3, 10), 'Union type operator "|" introduced in Python 3.10')
    def test_union_missing_type_function_310(self):
        class UnionMissingTypeFunctionTap(Tap):
            arg: int | float

        with self.assertRaises(ArgumentTypeError):
            UnionMissingTypeFunctionTap()


class AddArgumentTests(TestCase):
    def test_positional(self) -> None:
        class AddArgumentPositionalTap(IntegrationDefaultTap):
            def configure(self) -> None:
                self.add_argument("arg_str")

        arg_str = "positional"
        self.args = AddArgumentPositionalTap().parse_args([arg_str])

        self.assertEqual(self.args.arg_str, arg_str)

    def test_positional_ordering(self) -> None:
        class AddArgumentPositionalOrderingTap(IntegrationDefaultTap):
            def configure(self) -> None:
                self.add_argument("arg_str")
                self.add_argument("arg_int")
                self.add_argument("arg_float")

        arg_str = "positional"
        arg_int = "5"
        arg_float = "1.1"
        self.args = AddArgumentPositionalOrderingTap().parse_args([arg_str, arg_int, arg_float])

        arg_int = int(arg_int)
        arg_float = float(arg_float)

        self.assertEqual(self.args.arg_str, arg_str)
        self.assertEqual(self.args.arg_int, arg_int)
        self.assertEqual(self.args.arg_float, arg_float)

    def test_one_dash(self) -> None:
        class AddArgumentOneDashTap(IntegrationDefaultTap):
            def configure(self) -> None:
                self.add_argument("-arg_str")

        arg_str = "one_dash"
        self.args = AddArgumentOneDashTap().parse_args(["-arg_str", arg_str])

        self.assertEqual(self.args.arg_str, arg_str)

    def test_two_dashes(self) -> None:
        class AddArgumentTwoDashesTap(IntegrationDefaultTap):
            def configure(self) -> None:
                self.add_argument("--arg_str")

        arg_str = "two_dashes"
        self.args = AddArgumentTwoDashesTap().parse_args(["--arg_str", arg_str])

        self.assertEqual(self.args.arg_str, arg_str)

    def test_one_and_two_dashes(self) -> None:
        class AddArgumentOneAndTwoDashesTap(IntegrationDefaultTap):
            def configure(self) -> None:
                self.add_argument("-a", "--arg_str")

        arg_str = "one_or_two_dashes"
        self.args = AddArgumentOneAndTwoDashesTap().parse_args(["-a", arg_str])

        self.assertEqual(self.args.arg_str, arg_str)

        self.args = AddArgumentOneAndTwoDashesTap().parse_args(["--arg_str", arg_str])

        self.assertEqual(self.args.arg_str, arg_str)

    def test_not_class_variable(self) -> None:
        class AddArgumentNotClassVariableTap(IntegrationDefaultTap):
            def configure(self) -> None:
                self.add_argument("--non_class_arg")

        arg_str = "non_class_arg"
        self.tap = AddArgumentNotClassVariableTap()
        self.assertTrue("non_class_arg" in self.tap._get_argument_names())
        self.args = self.tap.parse_args(["--non_class_arg", arg_str])

        self.assertEqual(self.args.non_class_arg, arg_str)

    def test_complex_type(self) -> None:
        class AddArgumentComplexTypeTap(IntegrationDefaultTap):
            arg_person: Person = Person("tap")
            arg_person_required: Person
            arg_person_untyped = Person("tap untyped")

            def configure(self) -> None:
                self.add_argument("--arg_person", type=Person)
                self.add_argument("--arg_person_required", type=Person)
                self.add_argument("--arg_person_untyped", type=Person)

        arg_person_required = Person("hello, it's me")

        args = AddArgumentComplexTypeTap().parse_args(["--arg_person_required", arg_person_required.name,])
        self.assertEqual(args.arg_person, Person("tap"))
        self.assertEqual(args.arg_person_required, arg_person_required)
        self.assertEqual(args.arg_person_untyped, Person("tap untyped"))

        arg_person = Person("hi there")
        arg_person_untyped = Person("heyyyy")
        args = AddArgumentComplexTypeTap().parse_args(
            [
                "--arg_person",
                arg_person.name,
                "--arg_person_required",
                arg_person_required.name,
                "--arg_person_untyped",
                arg_person_untyped.name,
            ]
        )
        self.assertEqual(args.arg_person, arg_person)
        self.assertEqual(args.arg_person_required, arg_person_required)
        self.assertEqual(args.arg_person_untyped, arg_person_untyped)

    def test_repeat_default(self) -> None:
        class AddArgumentRepeatDefaultTap(IntegrationDefaultTap):
            def configure(self) -> None:
                self.add_argument("--arg_str", default=IntegrationDefaultTap.arg_str)

        args = AddArgumentRepeatDefaultTap().parse_args([])
        self.assertEqual(args.arg_str, IntegrationDefaultTap.arg_str)

    def test_conflicting_default(self) -> None:
        class AddArgumentConflictingDefaultTap(IntegrationDefaultTap):
            def configure(self) -> None:
                self.add_argument("--arg_str", default="yo dude")

        args = AddArgumentConflictingDefaultTap().parse_args([])
        self.assertEqual(args.arg_str, "yo dude")

    # TODO: this
    def test_repeat_required(self) -> None:
        pass

    # TODO: this
    def test_conflicting_required(self) -> None:
        pass

    def test_repeat_type(self) -> None:
        class AddArgumentRepeatTypeTap(IntegrationDefaultTap):
            def configure(self) -> None:
                self.add_argument("--arg_int", type=int)

        args = AddArgumentRepeatTypeTap().parse_args([])
        self.assertEqual(type(args.arg_int), int)
        self.assertEqual(args.arg_int, IntegrationDefaultTap.arg_int)

        arg_int = "99"
        args = AddArgumentRepeatTypeTap().parse_args(["--arg_int", arg_int])
        arg_int = int(arg_int)
        self.assertEqual(type(args.arg_int), int)
        self.assertEqual(args.arg_int, arg_int)

    def test_conflicting_type(self) -> None:
        class AddArgumentConflictingTypeTap(IntegrationDefaultTap):
            def configure(self) -> None:
                self.add_argument("--arg_int", type=str)

        arg_int = "yo dude"
        args = AddArgumentConflictingTypeTap().parse_args(["--arg_int", arg_int])
        self.assertEqual(type(args.arg_int), str)
        self.assertEqual(args.arg_int, arg_int)

    # TODO
    def test_repeat_help(self) -> None:
        pass

    # TODO
    def test_conflicting_help(self) -> None:
        pass

    def test_repeat_nargs(self) -> None:
        class AddArgumentRepeatNargsTap(IntegrationDefaultTap):
            def configure(self) -> None:
                self.add_argument("--arg_list_str", nargs="*")

        arg_list_str = ["hi", "there", "person", "123"]
        args = AddArgumentRepeatNargsTap().parse_args(["--arg_list_str", *arg_list_str])
        self.assertEqual(args.arg_list_str, arg_list_str)

    def test_conflicting_nargs(self) -> None:
        class AddArgumentConflictingNargsTap(IntegrationDefaultTap):
            def configure(self) -> None:
                self.add_argument("--arg_list_str", nargs=3)

        arg_list_str = ["hi", "there", "person", "123"]

        with self.assertRaises(SystemExit):
            AddArgumentConflictingNargsTap().parse_args(["--arg_list_str", *arg_list_str])

    def test_repeat_action(self) -> None:
        class AddArgumentRepeatActionTap(IntegrationDefaultTap):
            def configure(self) -> None:
                self.add_argument("--arg_bool_false", action="store_true", default=False)

        args = AddArgumentRepeatActionTap().parse_args([])
        self.assertEqual(args.arg_bool_false, False)

        args = AddArgumentRepeatActionTap().parse_args(["--arg_bool_false"])
        self.assertEqual(args.arg_bool_false, True)

    def test_conflicting_action(self) -> None:
        class AddArgumentConflictingActionTap(IntegrationDefaultTap):
            def configure(self) -> None:
                self.add_argument("--arg_bool_false", action="store_false", default=True)

        args = AddArgumentConflictingActionTap().parse_args([])
        self.assertEqual(args.arg_bool_false, True)

        args = AddArgumentConflictingActionTap().parse_args(["--arg_bool_false"])
        self.assertEqual(args.arg_bool_false, False)

    def test_add_argument_post_initialization(self) -> None:
        args = IntegrationDefaultTap()

        with self.assertRaises(ValueError):
            args.add_argument("--arg")


class KnownTap(Tap):
    arg_int: int = 2


class ParseKnownArgsTests(TestCase):
    arg_int = 3
    arg_float = 3.3

    def test_all_known(self) -> None:
        args = KnownTap().parse_args(["--arg_int", str(self.arg_int)], known_only=True)
        self.assertEqual(args.arg_int, self.arg_int)
        self.assertEqual(args.extra_args, [])

    def test_some_known(self) -> None:
        args = KnownTap().parse_args(
            ["--arg_int", str(self.arg_int), "--arg_float", str(self.arg_float)], known_only=True
        )
        self.assertEqual(args.arg_int, self.arg_int)
        self.assertEqual(args.extra_args, ["--arg_float", "3.3"])

    def test_none_known(self) -> None:
        args = KnownTap().parse_args(["--arg_float", str(self.arg_float)], known_only=True)
        self.assertEqual(args.extra_args, ["--arg_float", "3.3"])


class DashedArgumentsTests(TestCase):
    def test_dashed_arguments(self) -> None:
        class DashedArgumentTap(Tap):
            arg: int = 10
            arg_u_ment: int = 10
            arg_you_mean_: int = 10

        args = DashedArgumentTap(underscores_to_dashes=True).parse_args(
            ["--arg", "11", "--arg-u-ment", "12", "--arg-you-mean-", "13",]
        )
        self.assertEqual(args.arg, 11)
        self.assertEqual(args.arg_u_ment, 12)
        self.assertEqual(args.arg_you_mean_, 13)

    def test_dashed_arguments_required_default(self) -> None:
        class RequiredDashTap(Tap):
            foo_arg: str = "foo_arg"
            foo_arg_2: Optional[int] = None

            def configure(self) -> None:
                self.add_argument("-f", "--foo-arg")
                self.add_argument("-f2", "--foo-arg-2")

        args = RequiredDashTap(underscores_to_dashes=True).parse_args(["-f", "foo"])
        self.assertEqual(args.foo_arg, "foo")
        self.assertEqual(args.foo_arg_2, None)

        args = RequiredDashTap(underscores_to_dashes=True).parse_args(["-f2", "2"])
        self.assertEqual(args.foo_arg, "foo_arg")
        self.assertEqual(args.foo_arg_2, 2)

    def test_mismatch_dash_underscore_config_vs_annotations(self) -> None:
        class MismatchTap(Tap):
            arg_arg_arg: int

            def configure(self):
                self.add_argument("--arg-arg_arg", "-arg", type=str)

        args = MismatchTap(underscores_to_dashes=True).parse_args("--arg-arg-arg aarg".split())
        self.assertEqual(args.arg_arg_arg, "aarg")


class ParseExplicitBoolArgsTests(TestCase):
    def setUp(self) -> None:
        def test_bool_cases(cls):
            for true in ["True", "true", "T", "t", "1"]:
                arg = cls(explicit_bool=True).parse_args(["--is_gpu", true])
                self.assertTrue(arg.is_gpu)

            for false in ["False", "false", "F", "f", "0"]:
                arg = cls(explicit_bool=True).parse_args(["--is_gpu", false])
                self.assertFalse(arg.is_gpu)

        self.test_bool_cases = test_bool_cases

    def test_explicit_bool(self):
        class ExplicitBoolTap(Tap):
            is_gpu: bool

        with self.assertRaises(SystemExit):
            ExplicitBoolTap(explicit_bool=True).parse_args(["--is_gpu"])

        with self.assertRaises(SystemExit):
            ExplicitBoolTap(explicit_bool=True).parse_args([])

        self.test_bool_cases(ExplicitBoolTap)

    def test_explicit_bool_false(self):
        class ExplicitBoolFalseTap(Tap):
            is_gpu: bool = False

        self.test_bool_cases(ExplicitBoolFalseTap)

    def test_explicit_bool_true(self):
        class ExplicitBoolTrueTap(Tap):
            is_gpu: bool = True

        self.test_bool_cases(ExplicitBoolTrueTap)


class SetTests(TestCase):
    def test_set_non_set_default(self):
        class SetNonSetTap(Tap):
            set_1: Set[str] = None
            set_2: Set[int] = 3.7

        args = SetNonSetTap().parse_args([])
        self.assertEqual(args.set_1, None)
        self.assertEqual(args.set_2, 3.7)


class TupleTests(TestCase):
    def test_tuple_empty(self):
        tup_arg = ("three", "four", "ten")
        tup_default_arg = (1, 2, "5")

        class TupleEmptyTap(Tap):
            tup: Tuple
            tup_default: Tuple = tup_default_arg

        args = TupleEmptyTap().parse_args(["--tup", *tup_arg])

        self.assertEqual(args.tup, tup_arg)
        self.assertEqual(args.tup_default, tup_default_arg)

    def test_tuple_one(self):
        class TupleOneTap(Tap):
            tup_str: Tuple[str]
            tup_int: Tuple[int]
            tup_float: Tuple[float]
            tup_bool: Tuple[bool]

        arg_str = "hello"
        arg_int = 445
        arg_float = 7.9
        arg_bool = "tru"

        args = TupleOneTap().parse_args(
            ["--tup_str", arg_str, "--tup_int", str(arg_int), "--tup_float", str(arg_float), "--tup_bool", arg_bool]
        )

        self.assertEqual(args.tup_str, (arg_str,))
        self.assertEqual(args.tup_int, (arg_int,))
        self.assertEqual(args.tup_float, (arg_float,))
        self.assertEqual(args.tup_bool, (True,))

    def test_tuple_multi(self):
        class TupleMultiTap(Tap):
            tup: Tuple[str, int, float, bool, int, int, bool, bool, bool]

        input_args = ("hi there", -1, -1.0, "fAlS", 100, 1000, "false", "0", "1")
        true_args = ("hi there", -1, -1.0, False, 100, 1000, False, False, True)

        args = TupleMultiTap().parse_args(["--tup", *[str(arg) for arg in input_args]])

        self.assertEqual(args.tup, true_args)

    def test_infinite_tuple(self):
        class UnboundedTupleTap(Tap):
            tup_str: Tuple[str, ...]
            tup_int: Tuple[int, ...]
            tup_float: Tuple[float, ...]
            tup_bool: Tuple[bool, ...]

        arg_str = ("hi there", "hello hi bye")
        arg_int = (2, 3, 4)
        arg_float = (-1.0, 0.0, 2.3, 4.7)
        arg_bool = ("false", "true", "0", "1", "tru")
        arg_bool_true = (False, True, False, True, True)

        args = UnboundedTupleTap().parse_args(
            [
                "--tup_str",
                *arg_str,
                "--tup_int",
                *[str(arg) for arg in arg_int],
                "--tup_float",
                *[str(arg) for arg in arg_float],
                "--tup_bool",
                *arg_bool,
            ]
        )

        self.assertEqual(args.tup_str, arg_str)
        self.assertEqual(args.tup_int, arg_int)
        self.assertEqual(args.tup_float, arg_float)
        self.assertEqual(args.tup_bool, arg_bool_true)

    def test_tuple_class(self):
        class Dummy:
            def __init__(self, x):
                self.x = x

            def __eq__(self, other: "Dummy"):
                return isinstance(other, type(self)) and self.x == other.x

            def __str__(self):
                return f"Dummy({self.x})"

        class TupleClassTap(Tap):
            tup: Tuple[int, str, Dummy, Dummy]

        input_args = ("1", "2", "3", "4")
        true_args = (1, "2", Dummy("3"), Dummy("4"))

        args = TupleClassTap().parse_args(["--tup", *input_args])

        self.assertEqual(args.tup, true_args)

    def test_tuple_literally_three(self):
        class LiterallyOne(Tap):
            tup: Tuple[Literal[3]]

        input_args = ("3",)
        true_args = (3,)

        args = LiterallyOne().parse_args(["--tup", *input_args])

        self.assertEqual(args.tup, true_args)

        with self.assertRaises(SystemExit):
            LiterallyOne().parse_args(["--tup", "5"])

    def test_tuple_literally_two(self):
        class LiterallyTwo(Tap):
            tup: Tuple[Literal["two", 2], Literal[2, "too"]]

        input_args = ("2", "too")
        true_args = (2, "too")

        args = LiterallyTwo().parse_args(["--tup", *input_args])

        self.assertEqual(args.tup, true_args)

        with self.assertRaises(SystemExit):
            LiterallyTwo().parse_args(["--tup", "2"])

        with self.assertRaises(SystemExit):
            LiterallyTwo().parse_args(["--tup", "2", "3"])

        with self.assertRaises(SystemExit):
            LiterallyTwo().parse_args(["--tup", "too", "two"])

    def test_tuple_literally_infinity(self):
        class LiterallyInfinity(Tap):
            tup: Tuple[Literal["infinity", "∞"], ...]

        input_args = ("∞", "infinity", "∞")
        true_args = input_args

        args = LiterallyInfinity().parse_args(["--tup", *input_args])

        self.assertEqual(args.tup, true_args)

        with self.assertRaises(SystemExit):
            LiterallyInfinity().parse_args(["--tup", "8"])

    def test_tuple_wrong_type_fails(self):
        class TupleTapTypeFails(Tap):
            tup: Tuple[int]

        with self.assertRaises(SystemExit):
            TupleTapTypeFails().parse_args(["--tup", "tomato"])

    def test_tuple_wrong_num_args_fails(self):
        class TupleTapArgsFails(Tap):
            tup: Tuple[int]

        with self.assertRaises(SystemExit):
            TupleTapArgsFails().parse_args(["--tup", "1", "1"])

    def test_tuple_wrong_order_fails(self):
        class TupleTapOrderFails(Tap):
            tup: Tuple[int, str]

        with self.assertRaises(SystemExit):
            TupleTapOrderFails().parse_args(["--tup", "seven", "1"])

    def test_empty_tuple_fails(self):
        class EmptyTupleTap(Tap):
            tup_str: Tuple[()]

        arg_str = ("hi there", "hello hi bye")

        args = EmptyTupleTap().parse_args(["--tup_str", *arg_str,])

        self.assertEqual(args.tup_str, arg_str)

    def test_tuple_non_tuple_default(self):
        class TupleNonTupleTap(Tap):
            tup_1: Tuple[int, str] = None
            tup_2: Tuple[str, int] = 3

        args = TupleNonTupleTap().parse_args([])
        self.assertEqual(args.tup_1, None)
        self.assertEqual(args.tup_2, 3)


class TestAsDict(TestCase):
    def test_as_dict_simple(self):
        class SimpleTap(Tap):
            a: str
            b = 1
            c: bool = True
            d: Tuple[str, ...] = ("hi", "bob")
            e: Optional[int] = None
            f: Set[int] = {1}

        args = SimpleTap().parse_args(["--a", "hi"])
        as_dict_res = {"d": ("hi", "bob"), "b": 1, "c": True, "f": {1}, "e": None, "a": "hi"}
        self.assertEqual(args.as_dict(), as_dict_res)

    def test_as_dict_more_init_args(self):
        class InitArgsTap(Tap):
            a: str
            b = 1

            def __init__(self, g):
                super(InitArgsTap, self).__init__()
                self.g = g

        args = InitArgsTap("GG").parse_args(["--a", "hi"])
        as_dict_res = {"b": 1, "a": "hi", "g": "GG"}
        self.assertEqual(args.as_dict(), as_dict_res)

    def test_as_dict_add_arguments(self):
        class AddArgumentsTap(Tap):
            a: str
            b = 1

            def configure(self):
                self.add_argument("-arg_name", "--long_arg_name")

        args = AddArgumentsTap().parse_args(["--a", "hi", "--long_arg_name", "arg"])

        as_dict_res = {"a": "hi", "b": 1, "long_arg_name": "arg"}
        self.assertEqual(args.as_dict(), as_dict_res)

    def test_as_dict_manual_extra_args(self):
        class ManualExtraArgsTap(Tap):
            a: str
            b: int = 1

        args = ManualExtraArgsTap()
        args.c = 7
        args = args.parse_args(["--a", "hi"])
        args.d = "big"

        as_dict_res = {"a": "hi", "b": 1, "c": 7, "d": "big"}
        self.assertEqual(args.as_dict(), as_dict_res)

    def test_as_dict_property(self):
        class PropertyTap(Tap):
            a: str
            b: int = 1

            @property
            def pi(self):
                return 3.14

        args = PropertyTap()
        args = args.parse_args(["--a", "hi"])

        as_dict_res = {"a": "hi", "b": 1, "pi": 3.14}
        self.assertEqual(args.as_dict(), as_dict_res)

    def test_as_dict_superclass_property(self):
        class SuperPropertyTap(Tap):
            a: str

            @property
            def pi(self):
                return 3.14

        class SubPropertyTap(SuperPropertyTap):
            b: int = 1

        args = SubPropertyTap()
        args = args.parse_args(["--a", "hi"])

        as_dict_res = {"a": "hi", "b": 1, "pi": 3.14}
        self.assertEqual(args.as_dict(), as_dict_res)


class TestFromDict(TestCase):
    def test_from_dict_simple(self):
        class SimpleFromDictTap(Tap):
            a: str
            d: Tuple[str, ...]
            b = 1
            c: bool = True
            e: Optional[int] = None
            f: Set[int] = {1}

        args = SimpleFromDictTap().parse_args(["--a", "hi", "--d", "a", "b", "--e", "7"])
        d = args.as_dict()

        new_args = SimpleFromDictTap()
        new_args.from_dict(d)

        self.assertEqual(new_args.as_dict(), args.as_dict())

    def test_from_dict_returns_self(self):
        class FromDictReturnTap(Tap):
            a: str

        args = FromDictReturnTap().parse_args(["--a", "hi"])
        d = args.as_dict()

        new_args = FromDictReturnTap().from_dict(d)

        self.assertEqual(new_args.as_dict(), args.as_dict())

    def test_from_dict_fails_without_required(self):
        class SimpleFromDictTap(Tap):
            a: str
            d: Tuple[str, ...]
            b = 1
            c: bool = True
            e: Optional[int] = None
            f: Set[int] = {1}

        args = SimpleFromDictTap().parse_args(["--a", "hi", "--d", "a", "b", "--e", "7"])
        d = args.as_dict()
        d.pop("a")

        new_args = SimpleFromDictTap()

        with self.assertRaises(ValueError):
            new_args.from_dict(d)


class TestStoringTap(TestCase):
    def test_save_load_simple(self):
        class SimpleSaveLoadTap(Tap):
            a: str
            b = 1
            c: bool = True

        args = SimpleSaveLoadTap().parse_args(["--a", "hi"])

        with TemporaryDirectory() as temp_dir:
            fname = os.path.join(temp_dir, "args.json")
            args.save(fname)
            new_args = SimpleSaveLoadTap()
            new_args.load(fname)

        output = {"a": "hi", "b": 1, "c": True}
        self.assertEqual(output, new_args.as_dict())

    def test_save_load_return(self):
        class SaveLoadReturnTap(Tap):
            a: str

        args = SaveLoadReturnTap().parse_args(["--a", "hi"])

        with TemporaryDirectory() as temp_dir:
            fname = os.path.join(temp_dir, "args.json")
            args.save(fname)
            new_args = SaveLoadReturnTap().load(fname)

        output = {"a": "hi"}
        self.assertEqual(output, new_args.as_dict())

    def test_save_load_complex(self):
        class ComplexSaveLoadTap(Tap):
            a: str
            d: Tuple[str, ...]
            b = 1
            c: bool = True
            e: Optional[int] = None
            f: Set[int] = {1}
            g: Person = Person("tappy")

            def configure(self) -> None:
                self.add_argument("--g", type=Person)

        args = ComplexSaveLoadTap().parse_args(["--a", "hi", "--d", "a", "b", "--e", "7", "--g", "tapper"])

        with TemporaryDirectory() as temp_dir:
            fname = os.path.join(temp_dir, "args.json")
            args.save(fname)
            new_args = ComplexSaveLoadTap()
            new_args.load(fname)

        output = {"e": 7, "f": {1}, "d": ("a", "b"), "c": True, "a": "hi", "b": 1, "g": Person("tapper")}
        self.assertEqual(output, new_args.as_dict())

    def test_save_load_property(self):
        class PropertySaveLoadTap(Tap):
            a: str = "hello"

            def __init__(self):
                super(PropertySaveLoadTap, self).__init__()
                self._prop1 = 1
                self._prop2 = "hi"

            @property
            def prop1(self):
                return self._prop1

            @property
            def prop2(self):
                return self._prop2

            @prop2.setter
            def prop2(self, prop2):
                self._prop2 = prop2

            def process_args(self) -> None:
                self._prop1 = 2
                self.prop2 = "bye"

        args = PropertySaveLoadTap().parse_args([])

        with TemporaryDirectory() as temp_dir:
            fname = os.path.join(temp_dir, "args.json")
            args.save(fname)
            new_args = PropertySaveLoadTap()

            with self.assertRaises(AttributeError):
                new_args.load(fname)  # because trying to set unsettable prop1

            new_args.load(fname, skip_unsettable=True)

        output = {"a": "hello", "prop1": 1, "prop2": "bye"}
        self.assertEqual(output, new_args.as_dict())


class TestDeepCopy(TestCase):
    def test_deep_copy(self):
        class DeepCopyTap(Tap):
            a: str
            d: Tuple[str, ...]
            b = 1
            c: bool = True
            e: Optional[int] = None
            f: Set[int] = {1}
            g: Person = Person("tappy")

            def configure(self) -> None:
                self.add_argument("--g", type=Person)

            def __eq__(self, other):
                return isinstance(other, DeepCopyTap) and self.as_dict() == other.as_dict()

        args = DeepCopyTap().parse_args(["--a", "hi", "--d", "a", "b", "--e", "7", "--g", "tapper"])

        copied_args = deepcopy(args)
        self.assertEqual(args, copied_args)

        self.assertEqual(args.g, copied_args.g)
        args.g.name = "tap out"  # Ensure the deepcopy was recursive and deepcopied objects
        self.assertNotEqual(args.g, copied_args.g)


class TestGetClassDict(TestCase):
    def test_get_class_dict(self):
        class GetClassDictTap(Tap):
            a: str
            b = 1
            d: Tuple[str, ...]
            c: bool = True
            e: Optional[int] = None
            f: Set[int] = {1}
            _hidden: int = 1

            @classmethod
            def my_class_method(cls, arg):
                return arg

            @staticmethod
            def my_static_method(arg):
                return arg

            @property
            def my_property(self):
                return 1

        args = GetClassDictTap().parse_args(["--a", "hi", "--d", "a", "b", "--e", "7"])
        result = {"b": 1, "c": True, "e": None, "f": {1}}
        self.assertEqual(args._get_class_dict(), result)


class TestPositionalArguments(TestCase):
    def test_underscores_to_dashes_does_not_modify_positional(self):
        class PositionalTap(Tap):
            under_score: int

            def configure(self):
                self.add_argument("under_score")

        args = PositionalTap(underscores_to_dashes=True).parse_args(["1"])

        self.assertEqual(args.under_score, 1)

    def test_positional_maintains_types(self):
        class PositionalTap(Tap):
            a: int

            def configure(self) -> None:
                self.add_argument("a")

        args = PositionalTap().parse_args(["1"])

        self.assertEqual(args.a, 1)


class TestPickle(TestCase):
    def test_pickle(self):
        args = IntegrationDefaultTap().parse_args([])
        pickle_str = pickle.dumps(args)
        loaded_args: IntegrationDefaultTap = pickle.loads(pickle_str)
        self.assertEqual(loaded_args.as_dict(), args.as_dict())


class TestParserDescription(TestCase):
    def test_root_parser_description(self):
        class RootParser(Tap):
            """<Root Parser>"""

            field: str = "1"

        root_parser = RootParser()
        help_info = root_parser.format_help()
        self.assertIn("<Root Parser>", help_info)

    def test_sub_parser_description(self):
        class SubParser(Tap):
            """<Sub Parser>"""

            sub_field: str = "2"

        class RootParser(Tap):
            """<Root Parser>"""

            field: str = "1"

            def configure(self):
                self.add_subparsers(help="All sub parser")
                self.add_subparser("sub", SubParser)

        help_desc = RootParser().format_help()
        self.assertIn("<Sub Parser>", help_desc)
        self.assertIn("<Root Parser>", help_desc)

    def test_empty_docstring_should_be_empty(self):
        class NoDesc(Tap):
            field: str = "a"

        root_parser = NoDesc()
        help_info = root_parser.format_help()
        help_flag = "[-h]"
        desc_start = help_info.index(help_flag) + len(help_flag)
        desc_end = help_info.index("option")
        desc = help_info[desc_start:desc_end].strip()
        self.assertEqual(desc, "")

    def test_manual_description_still_works(self):
        class NoDesc(Tap):
            field: str = "a"

        expected_help_desc = "I exist"
        help_desc = NoDesc(description=expected_help_desc).format_help()
        self.assertIn(expected_help_desc, help_desc)

    def test_manual_description_overwrites_docstring(self):
        class Desc(Tap):
            """I do not exist"""

            field: str = "a"

        expected_help_desc = "I exist"
        help_desc = Desc(description=expected_help_desc).format_help()
        self.assertIn(expected_help_desc, help_desc)
        self.assertNotIn("I do not exist", help_desc)


class TestDefaultImmutability(TestCase):
    def test_default_immutability(self):
        holy_hand_grenada = [1, 2, 5]  # no, 3 sir!

        class DefaultImmutabilityTap(Tap):
            array: List[int] = holy_hand_grenada

        args = DefaultImmutabilityTap().parse_args([])
        self.assertEqual(args.array, holy_hand_grenada)

        holy_hand_grenada[2] = 3
        self.assertEqual(args.array, [1, 2, 5])

    def test_default_immutability_configure(self):
        holy_hand_grenada = [1, 2, 5]

        class DefaultImmutabilityTap(Tap):
            array: List[int]

            def configure(self) -> None:
                self.add_argument("--array", default=holy_hand_grenada)

        args = DefaultImmutabilityTap().parse_args([])
        self.assertEqual(args.array, holy_hand_grenada)

        holy_hand_grenada[2] = 3
        self.assertEqual(args.array, [1, 2, 5])


if __name__ == "__main__":
    unittest.main()
