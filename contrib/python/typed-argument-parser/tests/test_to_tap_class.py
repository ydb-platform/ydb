"""
Tests `tap.to_tap_class`.
"""

from contextlib import redirect_stdout, redirect_stderr
import dataclasses
import io
import re
import sys
from typing import Any, Callable, List, Literal, Optional, Type, Union

import pytest

from tap import to_tap_class, Tap
from tap.utils import type_to_str


try:
    import pydantic
except ModuleNotFoundError:
    _IS_PYDANTIC_V1 = None
else:
    _IS_PYDANTIC_V1 = pydantic.VERSION.startswith("1.")


# To properly test the help message, we need to know how argparse formats it. It changed from 3.9 -> 3.10 -> 3.13
_OPTIONS_TITLE = "options" if not sys.version_info < (3, 10) else "optional arguments"
_ARG_LIST_DOTS = "..."
_ARG_WITH_ALIAS = (
    "-arg, --argument_with_really_long_name ARGUMENT_WITH_REALLY_LONG_NAME"
    if not sys.version_info < (3, 13)
    else "-arg ARGUMENT_WITH_REALLY_LONG_NAME, --argument_with_really_long_name ARGUMENT_WITH_REALLY_LONG_NAME"
)


@dataclasses.dataclass
class _Args:
    """
    These are the arguments which every type of class or function must contain.
    """

    arg_int: int = dataclasses.field(metadata=dict(description="some integer"))
    arg_bool: bool = True
    arg_list: Optional[List[str]] = dataclasses.field(default=None, metadata=dict(description="some list of strings"))


def _monkeypatch_eq(cls):
    """
    Monkey-patches `cls.__eq__` to check that the attribute values are equal to a dataclass representation of them.
    """

    def _equality(self, other: _Args) -> bool:
        return _Args(self.arg_int, arg_bool=self.arg_bool, arg_list=self.arg_list) == other

    cls.__eq__ = _equality
    return cls


# Define a few different classes or functions which all take the same arguments (same by name, annotation, and default
# if not required)


def function(arg_int: int, arg_bool: bool = True, arg_list: Optional[List[str]] = None) -> _Args:
    """
    :param arg_int: some integer
    :param arg_list: some list of strings
    """
    return _Args(arg_int, arg_bool=arg_bool, arg_list=arg_list)


@_monkeypatch_eq
class Class:
    def __init__(self, arg_int: int, arg_bool: bool = True, arg_list: Optional[List[str]] = None):
        """
        :param arg_int: some integer
        :param arg_list: some list of strings
        """
        self.arg_int = arg_int
        self.arg_bool = arg_bool
        self.arg_list = arg_list


DataclassBuiltin = _Args


if _IS_PYDANTIC_V1 is None:
    pass  # will raise NameError if attempting to use DataclassPydantic or Model later
elif _IS_PYDANTIC_V1:
    # For Pydantic v1 data models, we rely on the docstring to get descriptions

    @_monkeypatch_eq
    @pydantic.dataclasses.dataclass
    class DataclassPydantic:
        """
        Dataclass (pydantic v1)

        :param arg_int: some integer
        :param arg_list: some list of strings
        """

        arg_int: int
        arg_bool: bool = True
        arg_list: Optional[List[str]] = None

    @_monkeypatch_eq
    class Model(pydantic.BaseModel):
        """
        Pydantic model (pydantic v1)

        :param arg_int: some integer
        :param arg_list: some list of strings
        """

        arg_int: int
        arg_bool: bool = True
        arg_list: Optional[List[str]] = None

else:
    # For pydantic v2 data models, we check the docstring and Field for the description

    @_monkeypatch_eq
    @pydantic.dataclasses.dataclass
    class DataclassPydantic:
        """
        Dataclass (pydantic)

        :param arg_list: some list of strings
        """

        # Mixing field types should be ok
        arg_int: int = pydantic.dataclasses.Field(description="some integer")
        arg_bool: bool = dataclasses.field(default=True)
        arg_list: Optional[List[str]] = pydantic.Field(default=None)

    @_monkeypatch_eq
    class Model(pydantic.BaseModel):
        """
        Pydantic model

        :param arg_int: some integer
        """

        # Mixing field types should be ok
        arg_int: int
        arg_bool: bool = dataclasses.field(default=True)
        arg_list: Optional[List[str]] = pydantic.dataclasses.Field(default=None, description="some list of strings")


@pytest.fixture(
    scope="module",
    params=[
        function,
        Class,
        DataclassBuiltin,
        DataclassBuiltin(
            1, arg_bool=False, arg_list=["these", "values", "don't", "matter"]
        ),  # to_tap_class also works on instances of data models. It ignores the attribute values
    ]
    + ([] if _IS_PYDANTIC_V1 is None else [DataclassPydantic, Model]),
    # NOTE: instances of DataclassPydantic and Model can be tested for pydantic v2 but not v1
)
def class_or_function_(request: pytest.FixtureRequest):
    """
    Parametrized class_or_function.
    """
    return request.param


# Define some functions which take a class or function and calls `tap.to_tap_class` on it to create a `tap.Tap`
# subclass (class, not instance)


def subclasser_simple(class_or_function: Any) -> Type[Tap]:
    """
    Plain subclass, does nothing extra.
    """
    return to_tap_class(class_or_function)


def subclasser_complex(class_or_function):
    """
    It's conceivable that someone has a data model, but they want to add more arguments or handling when running a
    script.
    """

    def to_number(string: str) -> Union[float, int]:
        return float(string) if "." in string else int(string)

    class TapSubclass(to_tap_class(class_or_function)):
        # You can supply additional arguments here
        argument_with_really_long_name: Union[float, int] = 3
        "This argument has a long name and will be aliased with a short one"

        def configure(self) -> None:
            # You can still add special argument behavior
            self.add_argument("-arg", "--argument_with_really_long_name", type=to_number)

        def process_args(self) -> None:
            # You can still validate and modify arguments
            if self.argument_with_really_long_name > 4:
                raise ValueError("argument_with_really_long_name cannot be > 4")

            # No auto-complete (and other niceties) for the super class attributes b/c this is a dynamic subclass. Sorry
            if self.arg_bool and self.arg_list is not None:
                self.arg_list.append("processed")

    return TapSubclass


def subclasser_subparser(class_or_function):
    class SubparserA(Tap):
        bar: int  # bar help

    class SubparserB(Tap):
        baz: Literal["X", "Y", "Z"]  # baz help

    class TapSubclass(to_tap_class(class_or_function)):
        foo: bool = False  # foo help

        def configure(self):
            self.add_subparsers(help="sub-command help")
            self.add_subparser("a", SubparserA, help="a help", description="Description (a)")
            self.add_subparser("b", SubparserB, help="b help")

    return TapSubclass


# Test that the subclasser parses the args correctly or raises the correct error.
# The subclassers are tested separately b/c the parametrizaiton of args_string_and_arg_to_expected_value depends on the
# subclasser.
# First, some helper functions.


def _test_raises_system_exit(tap: Tap, args_string: str) -> str:
    is_help = (
        args_string.endswith("-h")
        or args_string.endswith("--help")
        or " -h " in args_string
        or " --help " in args_string
    )
    f = io.StringIO()
    with redirect_stdout(f) if is_help else redirect_stderr(f):
        with pytest.raises(SystemExit):
            tap.parse_args(args_string.split())

    return f.getvalue()


def _test_subclasser(
    subclasser: Callable[[Any], Type[Tap]],
    class_or_function: Any,
    args_string_and_arg_to_expected_value: tuple[str, Union[dict[str, Any], BaseException]],
    test_call: bool = True,
):
    """
    Tests that the `subclasser` converts `class_or_function` to a `Tap` class which parses the argument string
    correctly.

    Setting `test_call=True` additionally tests that calling the `class_or_function` on the parsed arguments works.
    """
    args_string, arg_to_expected_value = args_string_and_arg_to_expected_value
    TapSubclass = subclasser(class_or_function)
    assert issubclass(TapSubclass, Tap)
    tap = TapSubclass(description="Script description")

    if isinstance(arg_to_expected_value, SystemExit):
        stderr = _test_raises_system_exit(tap, args_string)
        assert re.search(str(arg_to_expected_value), stderr)
    elif isinstance(arg_to_expected_value, BaseException):
        expected_exception = arg_to_expected_value.__class__
        expected_error_message = str(arg_to_expected_value) or None
        with pytest.raises(expected_exception=expected_exception, match=expected_error_message):
            args = tap.parse_args(args_string.split())
    else:
        # args_string is a valid argument combo
        # Test that parsing works correctly
        args = tap.parse_args(args_string.split())
        assert arg_to_expected_value == args.as_dict()
        if test_call and callable(class_or_function):
            result = class_or_function(**args.as_dict())
            assert result == _Args(**arg_to_expected_value)


def _test_subclasser_message(
    subclasser: Callable[[Any], Type[Tap]],
    class_or_function: Any,
    message_expected: str,
    description: str = "Script description",
    args_string: str = "-h",
):
    """
    Tests that::

        subclasser(class_or_function)(description=description).parse_args(args_string.split())

    outputs `message_expected` to stdout, ignoring differences in whitespaces/newlines/tabs.
    """

    def replace_whitespace(string: str) -> str:
        return re.sub(r"\s+", " ", string).strip()  # FYI this line was written by an LLM

    TapSubclass = subclasser(class_or_function)
    tap = TapSubclass(description=description)
    message = _test_raises_system_exit(tap, args_string)
    # Standardize to ignore trivial differences due to terminal settings
    assert replace_whitespace(message) == replace_whitespace(message_expected)


# Test sublcasser_simple


@pytest.mark.parametrize(
    "args_string_and_arg_to_expected_value",
    [
        (
            "--arg_int 1 --arg_list x y z",
            {"arg_int": 1, "arg_bool": True, "arg_list": ["x", "y", "z"]},
        ),
        (
            "--arg_int 1 --arg_bool",
            {"arg_int": 1, "arg_bool": False, "arg_list": None},
        ),
        # The rest are invalid argument combos, as indicated by the 2nd elt being a BaseException instance
        (
            "--arg_list x y z --arg_bool",
            SystemExit("error: the following arguments are required: --arg_int"),
        ),
        (
            "--arg_int not_an_int --arg_list x y z --arg_bool",
            SystemExit("error: argument --arg_int: invalid int value: 'not_an_int'"),
        ),
    ],
)
def test_subclasser_simple(
    class_or_function_: Any, args_string_and_arg_to_expected_value: tuple[str, Union[dict[str, Any], BaseException]]
):
    _test_subclasser(subclasser_simple, class_or_function_, args_string_and_arg_to_expected_value)


def test_subclasser_simple_help_message(class_or_function_: Any):
    description = "Script description"
    help_message_expected = f"""
    usage: contrib-python-typed-argument-parser-tests --arg_int ARG_INT [--arg_bool] [--arg_list [ARG_LIST {_ARG_LIST_DOTS}]] [-h]

    {description}

    {_OPTIONS_TITLE}:
    --arg_int ARG_INT     (int, required) some integer
    --arg_bool            (bool, default=True)
    --arg_list [ARG_LIST {_ARG_LIST_DOTS}]
                            ({type_to_str(Optional[List[str]])}, default=None) some list of strings
    -h, --help            show this help message and exit
    """
    _test_subclasser_message(subclasser_simple, class_or_function_, help_message_expected, description=description)


# Test subclasser_complex


@pytest.mark.parametrize(
    "args_string_and_arg_to_expected_value",
    [
        (
            "--arg_int 1 --arg_list x y z",
            {
                "arg_int": 1,
                "arg_bool": True,
                "arg_list": ["x", "y", "z", "processed"],
                "argument_with_really_long_name": 3,
            },
        ),
        (
            "--arg_int 1 --arg_list x y z -arg 2",
            {
                "arg_int": 1,
                "arg_bool": True,
                "arg_list": ["x", "y", "z", "processed"],
                "argument_with_really_long_name": 2,
            },
        ),
        (
            "--arg_int 1 --arg_bool --argument_with_really_long_name 2.3",
            {
                "arg_int": 1,
                "arg_bool": False,
                "arg_list": None,
                "argument_with_really_long_name": 2.3,
            },
        ),
        # The rest are invalid argument combos, as indicated by the 2nd elt being a BaseException instance
        (
            "--arg_list x y z --arg_bool",
            SystemExit("error: the following arguments are required: --arg_int"),
        ),
        (
            "--arg_int 1 --arg_list x y z -arg not_a_float_or_int",
            SystemExit(
                "error: argument -arg/--argument_with_really_long_name: invalid to_number value: 'not_a_float_or_int'"
            ),
        ),
        (
            "--arg_int 1 --arg_list x y z -arg 5",  # Wrong value arg (aliases argument_with_really_long_name)
            ValueError("argument_with_really_long_name cannot be > 4"),
        ),
    ],
)
def test_subclasser_complex(
    class_or_function_: Any, args_string_and_arg_to_expected_value: tuple[str, Union[dict[str, Any], BaseException]]
):
    # Currently setting test_call=False b/c all data models except the pydantic Model don't accept extra args
    _test_subclasser(subclasser_complex, class_or_function_, args_string_and_arg_to_expected_value, test_call=False)


def test_subclasser_complex_help_message(class_or_function_: Any):
    description = "Script description"
    help_message_expected = f"""
    usage: contrib-python-typed-argument-parser-tests [-arg ARGUMENT_WITH_REALLY_LONG_NAME] --arg_int ARG_INT [--arg_bool]
                  [--arg_list [ARG_LIST {_ARG_LIST_DOTS}]] [-h]

    {description}

    {_OPTIONS_TITLE}:
    {_ARG_WITH_ALIAS}
                            ({type_to_str(Union[float, int])}, default=3) This argument has a long name and will be
                            aliased with a short one
    --arg_int ARG_INT     (int, required) some integer
    --arg_bool            (bool, default=True)
    --arg_list [ARG_LIST {_ARG_LIST_DOTS}]
                            ({type_to_str(Optional[List[str]])}, default=None) some list of strings
    -h, --help            show this help message and exit
    """
    _test_subclasser_message(subclasser_complex, class_or_function_, help_message_expected, description=description)


# Test subclasser_subparser


@pytest.mark.parametrize(
    "args_string_and_arg_to_expected_value",
    [
        (
            "--arg_int 1",
            {"arg_int": 1, "arg_bool": True, "arg_list": None, "foo": False},
        ),
        (
            "--arg_int 1 a --bar 2",
            {"arg_int": 1, "arg_bool": True, "arg_list": None, "bar": 2, "foo": False},
        ),
        (
            "--arg_int 1 --foo a --bar 2",
            {"arg_int": 1, "arg_bool": True, "arg_list": None, "bar": 2, "foo": True},
        ),
        (
            "--arg_int 1 b --baz X",
            {"arg_int": 1, "arg_bool": True, "arg_list": None, "baz": "X", "foo": False},
        ),
        (
            "--foo --arg_bool --arg_list x y z --arg_int 1 b --baz Y",
            {"arg_int": 1, "arg_bool": False, "arg_list": ["x", "y", "z"], "baz": "Y", "foo": True},
        ),
        # The rest are invalid argument combos, as indicated by the 2nd elt being a BaseException instance
        (
            "a --bar 1",
            SystemExit("error: the following arguments are required: --arg_int"),
        ),
        (
            "--arg_int not_an_int a --bar 1",
            SystemExit("error: argument --arg_int: invalid int value: 'not_an_int'"),
        ),
        (
            "--arg_int 1 --baz X --foo b",
            SystemExit(r"error: argument \{a,b}: invalid choice: 'X' \(choose from '?a'?, '?b'?\)"),
        ),
        (
            "--arg_int 1 b --baz X --foo",
            SystemExit("error: unrecognized arguments: --foo"),
        ),
        (
            "--arg_int 1 --foo b --baz A",
            SystemExit(r"""error: argument --baz: Value for variable "baz" must be one of \['X', 'Y', 'Z']."""),
        ),
    ],
)
def test_subclasser_subparser(
    class_or_function_: Any, args_string_and_arg_to_expected_value: tuple[str, Union[dict[str, Any], BaseException]]
):
    # Currently setting test_call=False b/c all data models except the pydantic Model don't accept extra args
    _test_subclasser(subclasser_subparser, class_or_function_, args_string_and_arg_to_expected_value, test_call=False)


@pytest.mark.parametrize(
    "args_string_and_description_and_expected_message",
    [
        (
            "-h",
            "Script description",
            f"""
            usage: contrib-python-typed-argument-parser-tests [--foo] --arg_int ARG_INT [--arg_bool] [--arg_list [ARG_LIST {_ARG_LIST_DOTS}]] [-h]
                          {{a,b}} ...

            Script description

            positional arguments:
            {{a,b}}               sub-command help
                a                   a help
                b                   b help

            {_OPTIONS_TITLE}:
            --foo                 (bool, default=False) foo help
            --arg_int ARG_INT     (int, required) some integer
            --arg_bool            (bool, default=True)
            --arg_list [ARG_LIST {_ARG_LIST_DOTS}]
                                    ({type_to_str(Optional[List[str]])}, default=None) some list of strings
            -h, --help            show this help message and exit
            """,
        ),
        (
            "a -h",
            "Description (a)",
            f"""
            usage: contrib-python-typed-argument-parser-tests a --bar BAR [-h]

            Description (a)

            {_OPTIONS_TITLE}:
            --bar BAR   (int, required) bar help
            -h, --help  show this help message and exit
            """,
        ),
        (
            "b -h",
            "",  # no description
            f"""
            usage: contrib-python-typed-argument-parser-tests b --baz {{X,Y,Z}} [-h]

            {_OPTIONS_TITLE}:
            --baz {{X,Y,Z}}  (Literal['X', 'Y', 'Z'], required) baz help
            -h, --help     show this help message and exit
            """,
        ),
    ],
)
def test_subclasser_subparser_help_message(
    class_or_function_: Any, args_string_and_description_and_expected_message: tuple[str, str, str]
):
    args_string, description, expected_message = args_string_and_description_and_expected_message
    _test_subclasser_message(
        subclasser_subparser, class_or_function_, expected_message, description=description, args_string=args_string
    )
