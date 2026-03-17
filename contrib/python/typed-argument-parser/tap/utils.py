from argparse import ArgumentParser, ArgumentTypeError
import ast
from base64 import b64encode, b64decode
import inspect
from io import StringIO
from json import JSONEncoder
import os
import pickle
import re
import subprocess
import sys
import textwrap
import tokenize
from typing import (
    Any,
    Callable,
    Generator,
    Iterable,
    Iterator,
    Literal,
    Optional,
    Union,
)
from typing_inspect import get_args as typing_inspect_get_args, get_origin as typing_inspect_get_origin
import warnings

if sys.version_info >= (3, 10):
    from types import UnionType

NO_CHANGES_STATUS = """nothing to commit, working tree clean"""
PRIMITIVES = (str, int, float, bool)
PathLike = Union[str, os.PathLike]


def check_output(command: list[str], suppress_stderr: bool = True, **kwargs) -> str:
    """Runs subprocess.check_output and returns the result as a string.

    :param command: A list of strings representing the command to run on the command line.
    :param suppress_stderr: Whether to suppress anything written to standard error.
    :return: The output of the command, converted from bytes to string and stripped.
    """
    with open(os.devnull, "w") as devnull:
        devnull = devnull if suppress_stderr else None
        output = subprocess.check_output(command, stderr=devnull, **kwargs).decode("utf-8").strip()
    return output


class GitInfo:
    """Class with helper methods for extracting information about a git repo."""

    def __init__(self, repo_path: PathLike):
        self.repo_path = repo_path

    def has_git(self) -> bool:
        """Returns whether git is installed.

        :return: True if git is installed, False otherwise.
        """
        try:
            output = check_output(["git", "rev-parse", "--is-inside-work-tree"], cwd=self.repo_path)
            return output == "true"
        except (FileNotFoundError, subprocess.CalledProcessError):
            return False

    def get_git_root(self) -> str:
        """Gets the root directory of the git repo where the command is run.

        :return: The root directory of the current git repo.
        """
        return check_output(["git", "rev-parse", "--show-toplevel"], cwd=self.repo_path)

    def get_git_version(self) -> tuple:
        """Gets the version of git.

        :return: The version of git, as a tuple of strings.

        Example:
        >>> get_git_version()
        (2, 17, 1) # for git version 2.17.1
        """
        raw = check_output(["git", "--version"])
        number_start_index = next(i for i, c in enumerate(raw) if c.isdigit())
        return tuple(int(num) for num in raw[number_start_index:].split(".") if num.isdigit())

    def get_git_url(self, commit_hash: bool = True) -> str:
        """Gets the https url of the git repo where the command is run.

        :param commit_hash: If True, the url links to the latest local git commit hash.
        If False, the url links to the general git url.
        :return: The https url of the current git repo or an empty string for a local repo.
        """
        # Get git url (either https or ssh)
        input_remote = (
            ["git", "remote", "get-url", "origin"]
            if self.get_git_version() >= (2, 0)
            else ["git", "config", "--get", "remote.origin.url"]
        )
        try:
            url = check_output(input_remote, cwd=self.repo_path)
        except subprocess.CalledProcessError as e:
            if e.returncode in {2, 128}:
                # https://git-scm.com/docs/git-remote#_exit_status
                # 2: The remote does not exist.
                # 128: The remote was not found.
                return ""
            raise e

        # Remove .git at end
        url = url[: -len(".git")]

        # Convert ssh url to https url
        m = re.search("git@(.+):", url)
        if m is not None:
            domain = m.group(1)
            path = url[m.span()[1] :]
            url = f"https://{domain}/{path}"

        if commit_hash:
            # Add tree and hash of current commit
            url = f"{url}/tree/{self.get_git_hash()}"

        return url

    def get_git_hash(self) -> str:
        """Gets the git hash of HEAD of the git repo where the command is run.

        :return: The git hash of HEAD of the current git repo.
        """
        return check_output(["git", "rev-parse", "HEAD"], cwd=self.repo_path)

    def has_uncommitted_changes(self) -> bool:
        """Returns whether there are uncommitted changes in the git repo where the command is run.

        :return: True if there are uncommitted changes in the current git repo, False otherwise.
        """
        status = check_output(["git", "status"], cwd=self.repo_path)

        return not status.endswith(NO_CHANGES_STATUS)


def type_to_str(type_annotation: Union[type, Any]) -> str:
    """Gets a string representation of the provided type.

    :param type_annotation: A type annotation, which is either a built-in type or a typing type.
    :return: A string representation of the type annotation.
    """
    # Built-in type
    if type(type_annotation) == type:
        return type_annotation.__name__

    # Typing type
    return str(type_annotation).replace("typing.", "")


def get_argument_name(*name_or_flags) -> str:
    """Gets the name of the argument.

    :param name_or_flags: Either a name or a list of option strings, e.g. foo or -f, --foo.
    :return: The name of the argument (extracted from name_or_flags).
    """
    if "-h" in name_or_flags or "--help" in name_or_flags:
        return "help"

    if len(name_or_flags) > 1:
        name_or_flags = tuple(n_or_f for n_or_f in name_or_flags if n_or_f.startswith("--"))

    if len(name_or_flags) != 1:
        raise ValueError(f"There should only be a single canonical name for argument {name_or_flags}!")

    return name_or_flags[0].lstrip("-")


def get_dest(*name_or_flags, **kwargs) -> str:
    """Gets the name of the destination of the argument.

    :param name_or_flags: Either a name or a list of option strings, e.g. foo or -f, --foo.
    :param kwargs: Keyword arguments.
    :return: The name of the argument (extracted from name_or_flags).
    """
    if "-h" in name_or_flags or "--help" in name_or_flags:
        return "help"

    return ArgumentParser().add_argument(*name_or_flags, **kwargs).dest


def is_option_arg(*name_or_flags) -> bool:
    """Returns whether the argument is an option arg (as opposed to a positional arg).

    :param name_or_flags: Either a name or a list of option strings, e.g. foo or -f, --foo.
    :return: True if the argument is an option arg, False otherwise.
    """
    return any(name_or_flag.startswith("-") for name_or_flag in name_or_flags)


def is_positional_arg(*name_or_flags) -> bool:
    """Returns whether the argument is a positional arg (as opposed to an optional arg).

    :param name_or_flags: Either a name or a list of option strings, e.g. foo or -f, --foo.
    :return: True if the argument is a positional arg, False otherwise.
    """
    return not is_option_arg(*name_or_flags)


def tokenize_source(source: str) -> Generator[tokenize.TokenInfo, None, None]:
    """Returns a generator for the tokens of the object's source code, given the source code."""
    return tokenize.generate_tokens(StringIO(source).readline)


def get_class_column(tokens: Iterable[tokenize.TokenInfo]) -> int:
    """Determines the column number for class variables in a class, given the tokens of the class."""
    first_line = 1
    for token_type, token, (start_line, start_column), (end_line, end_column), line in tokens:
        if token.strip() == "@":
            first_line += 1
        if start_line <= first_line or token.strip() == "":
            continue

        return start_column
    raise ValueError("Could not find any class variables in the class.")


def source_line_to_tokens(tokens: Iterable[tokenize.TokenInfo]) -> dict[int, list[dict[str, Union[str, int]]]]:
    """Extract a map from each line number to list of mappings providing information about each token."""
    line_to_tokens = {}
    for token_type, token, (start_line, start_column), (end_line, end_column), line in tokens:
        line_to_tokens.setdefault(start_line, []).append(
            {
                "token_type": token_type,
                "token": token,
                "start_line": start_line,
                "start_column": start_column,
                "end_line": end_line,
                "end_column": end_column,
                "line": line,
            }
        )

    return line_to_tokens


def get_subsequent_assign_lines(source_cls: str) -> tuple[set[int], set[int]]:
    """For all multiline assign statements, get the line numbers after the first line in the assignment.

    :param source_cls: The source code of the class.
    :return: A set of intermediate line numbers for multiline assign statements and a set of final line numbers.
    """
    # Parse source code using ast (with an if statement to avoid indentation errors)
    source = f"if True:\n{textwrap.indent(source_cls, ' ')}"
    body = ast.parse(source).body[0]

    # Set up warning message
    parse_warning = (
        "Could not parse class source code to extract comments. Comments in the help string may be incorrect."
    )

    # Check for correct parsing
    if not isinstance(body, ast.If):
        warnings.warn(parse_warning)
        return set(), set()

    # Extract if body
    if_body = body.body

    # Check for a single body
    if len(if_body) != 1:
        warnings.warn(parse_warning)
        return set(), set()

    # Extract class body
    cls_body = if_body[0]

    # Check for a single class definition
    if not isinstance(cls_body, ast.ClassDef):
        warnings.warn(parse_warning)
        return set(), set()

    # Get line numbers of assign statements
    intermediate_assign_lines = set()
    final_assign_lines = set()
    for node in cls_body.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            # Check if the end line number is found
            if node.end_lineno is None:
                warnings.warn(parse_warning)
                continue

            # Only consider multiline assign statements
            if node.end_lineno > node.lineno:
                # Get intermediate line number of assign statement excluding the first line (and minus 1 for the if statement)
                intermediate_assign_lines |= set(range(node.lineno, node.end_lineno - 1))

                # If multiline assign statement, get the line number of the last line (and minus 1 for the if statement)
                final_assign_lines.add(node.end_lineno - 1)

    return intermediate_assign_lines, final_assign_lines


def get_class_variables(cls: type) -> dict[str, dict[str, str]]:
    """Returns a dictionary mapping class variables to their additional information (currently just comments)."""
    # Get the source code and tokens of the class
    source_cls = inspect.getsource(cls)
    tokens = tuple(tokenize_source(source_cls))

    # Get mapping from line number to tokens
    line_to_tokens = source_line_to_tokens(tokens)

    # Get class variable column number
    class_variable_column = get_class_column(tokens)

    # For all multiline assign statements, get the line numbers after the first line of the assignment
    # This is used to avoid identifying comments in multiline assign statements
    intermediate_assign_lines, final_assign_lines = get_subsequent_assign_lines(source_cls)

    # Extract class variables
    class_variable = None
    variable_to_comment = {}
    for line, tokens in line_to_tokens.items():
        # If this is the final line of a multiline assign, extract any potential comments
        if line in final_assign_lines:
            # Find the comment (if it exists)
            for token in tokens:
                if token["token_type"] == tokenize.COMMENT:
                    # Leave out "#" and whitespace from comment
                    variable_to_comment[class_variable]["comment"] = token["token"][1:].strip()
                    break
            continue

        # Skip assign lines after the first line of multiline assign statements
        if line in intermediate_assign_lines:
            continue

        for i, token in enumerate(tokens):
            # Skip whitespace
            if token["token"].strip() == "":
                continue

            # Extract multiline comments
            if (
                class_variable is not None
                and token["token_type"] == tokenize.STRING
                and token["token"][:1] in {'"', "'"}
            ):
                sep = " " if variable_to_comment[class_variable]["comment"] else ""

                # Identify the quote character (single or double)
                quote_char = token["token"][:1]

                # Identify the number of quote characters at the start of the string
                num_quote_chars = len(token["token"]) - len(token["token"].lstrip(quote_char))

                # Remove the number of quote characters at the start of the string and the end of the string
                token["token"] = token["token"][num_quote_chars:-num_quote_chars]

                # Remove the unicode escape sequences (e.g. "\"")
                token["token"] = bytes(token["token"], encoding="ascii").decode("unicode-escape")

                # Add the token to the comment, stripping whitespace
                variable_to_comment[class_variable]["comment"] += sep + token["token"].strip()

            # Match class variable
            class_variable = None
            if (
                token["token_type"] == tokenize.NAME
                and token["start_column"] == class_variable_column
                and len(tokens) > i
                and tokens[i + 1]["token"] in ["=", ":"]
            ):

                class_variable = token["token"]
                variable_to_comment[class_variable] = {"comment": ""}

                # Find the comment (if it exists)
                for j in range(i + 1, len(tokens)):
                    if tokens[j]["token_type"] == tokenize.COMMENT:
                        # Leave out "#" and whitespace from comment
                        variable_to_comment[class_variable]["comment"] = tokens[j]["token"][1:].strip()
                        break

            break

    return variable_to_comment


def get_literals(literal: Literal, variable: str) -> tuple[Callable[[str], Any], list[type]]:
    """Extracts the values from a Literal type and ensures that the values are all primitive types."""
    literals = list(get_args(literal))

    if not all(isinstance(literal, PRIMITIVES) for literal in literals):
        raise ArgumentTypeError(
            f'The type for variable "{variable}" contains a literal'
            f"of a non-primitive type e.g. (str, int, float, bool).\n"
            f"Currently only primitive-typed literals are supported."
        )

    str_to_literal = {str(literal): literal for literal in literals}

    if len(literals) != len(str_to_literal):
        raise ArgumentTypeError("All literals must have unique string representations")

    def var_type(arg: str) -> Any:
        if arg not in str_to_literal:
            raise ArgumentTypeError(f'Value for variable "{variable}" must be one of {literals}.')

        return str_to_literal[arg]

    return var_type, literals


def boolean_type(flag_value: str) -> bool:
    """Convert a string to a boolean if it is a prefix of 'True' or 'False' (case insensitive) or is '1' or '0'."""
    if "true".startswith(flag_value.lower()) or flag_value == "1":
        return True
    if "false".startswith(flag_value.lower()) or flag_value == "0":
        return False
    raise ArgumentTypeError('Value has to be a prefix of "True" or "False" (case insensitive) or "1" or "0".')


class TupleTypeEnforcer:
    """The type argument to argparse for checking and applying types to Tuples."""

    def __init__(self, types: list[type], loop: bool = False):
        self.types = [boolean_type if t == bool else t for t in types]
        self.loop = loop
        self.index = 0

    def __call__(self, arg: str) -> Any:
        arg = self.types[self.index](arg)
        self.index += 1

        if self.loop:
            self.index %= len(self.types)

        return arg


class MockTuple:
    """Mock of a tuple needed to prevent JSON encoding tuples as lists."""

    def __init__(self, _tuple: tuple) -> None:
        self.tuple = _tuple


def _nested_replace_type(obj: Any, find_type: type, replace_type: type) -> Any:
    """Replaces any instance (including instances within lists, tuple, dict) of find_type with an instance of replace_type.

    Note: Tuples, lists, and dicts are NOT modified in place.
    Note: Does NOT do a nested search through objects besides tuples, lists, and dicts (e.g. sets).

    :param obj: The object to modify by replacing find_type instances with replace_type instances.
    :param find_type: The type to find in obj.
    :param replace_type: The type to used to replace find_type in obj.
    :return: A version of obj with all instances of find_type replaced by replace_type
    """
    if isinstance(obj, tuple):
        obj = tuple(_nested_replace_type(item, find_type, replace_type) for item in obj)

    elif isinstance(obj, list):
        obj = [_nested_replace_type(item, find_type, replace_type) for item in obj]

    elif isinstance(obj, dict):
        obj = {
            _nested_replace_type(key, find_type, replace_type): _nested_replace_type(value, find_type, replace_type)
            for key, value in obj.items()
        }

    if isinstance(obj, find_type):
        obj = replace_type(obj)

    return obj


def define_python_object_encoder(skip_unpicklable: bool = False) -> "PythonObjectEncoder":  # noqa F821
    class PythonObjectEncoder(JSONEncoder):
        """Stores parameters that are not JSON serializable as pickle dumps.

        See: https://stackoverflow.com/a/36252257
        """

        def iterencode(self, o: Any, _one_shot: bool = False) -> Iterator[str]:
            o = _nested_replace_type(o, tuple, MockTuple)
            return super(PythonObjectEncoder, self).iterencode(o, _one_shot)

        def default(self, obj: Any) -> Any:
            if isinstance(obj, set):
                return {"_type": "set", "_value": list(obj)}
            elif isinstance(obj, MockTuple):
                return {"_type": "tuple", "_value": list(obj.tuple)}

            try:
                return {
                    "_type": f"python_object (type = {obj.__class__.__name__})",
                    "_value": b64encode(pickle.dumps(obj)).decode("utf-8"),
                    "_string": str(obj),
                }
            except (pickle.PicklingError, TypeError, AttributeError) as e:
                if not skip_unpicklable:
                    raise ValueError(
                        f"Could not pickle this object: Failed with exception {e}\n"
                        f"If you would like to ignore unpicklable attributes set "
                        f"skip_unpickleable = True in save."
                    )
                else:
                    return {"_type": f"unpicklable_object {obj.__class__.__name__}", "_value": None}

    return PythonObjectEncoder


class UnpicklableObject:
    """A class that serves as a placeholder for an object that could not be pickled."""

    def __eq__(self, other):
        return isinstance(other, UnpicklableObject)


def as_python_object(dct: Any) -> Any:
    """The hooks that allow a parameter that is not JSON serializable to be loaded.

    See: https://stackoverflow.com/a/36252257
    """
    if "_type" in dct and "_value" in dct:
        _type, value = dct["_type"], dct["_value"]

        if _type == "tuple":
            return tuple(value)

        elif _type == "set":
            return set(value)

        elif _type.startswith("python_object"):
            return pickle.loads(b64decode(value.encode("utf-8")))

        elif _type.startswith("unpicklable_object"):
            return UnpicklableObject()

        else:
            raise ArgumentTypeError(f'Special type "{_type}" not supported for JSON loading.')

    return dct


def enforce_reproducibility(
    saved_reproducibility_data: Optional[dict[str, str]], current_reproducibility_data: dict[str, str], path: PathLike
) -> None:
    """Checks if reproducibility has failed and raises the appropriate error.

    :param saved_reproducibility_data: Reproducibility information loaded from a saved file.
    :param current_reproducibility_data: Reproducibility information from the current object.
    :param path: The path name of the file that is being loaded.
    """
    no_reproducibility_message = "Reproducibility not guaranteed"

    if saved_reproducibility_data is None:
        raise ValueError(
            f"{no_reproducibility_message}: Could not find reproducibility "
            f'information in args loaded from "{path}".'
        )

    if "git_url" not in saved_reproducibility_data:
        raise ValueError(f"{no_reproducibility_message}: Could not find " f'git url in args loaded from "{path}".')

    if "git_url" not in current_reproducibility_data:
        raise ValueError(f"{no_reproducibility_message}: Could not find " f"git url in current args.")

    if saved_reproducibility_data["git_url"] != current_reproducibility_data["git_url"]:
        raise ValueError(
            f"{no_reproducibility_message}: Differing git url/hash "
            f'between current args and args loaded from "{path}".'
        )

    if saved_reproducibility_data["git_has_uncommitted_changes"]:
        raise ValueError(f"{no_reproducibility_message}: Uncommitted changes " f'in args loaded from "{path}".')

    if current_reproducibility_data["git_has_uncommitted_changes"]:
        raise ValueError(f"{no_reproducibility_message}: Uncommitted changes " f"in current args.")


# TODO: remove this once typing_inspect.get_origin is fixed for Python 3.9 and 3.10
# https://github.com/ilevkivskyi/typing_inspect/issues/64
# https://github.com/ilevkivskyi/typing_inspect/issues/65
def get_origin(tp: Any) -> Any:
    """Same as typing_inspect.get_origin but fixes unparameterized generic types like Set."""
    origin = typing_inspect_get_origin(tp)

    if origin is None:
        origin = tp

    if sys.version_info >= (3, 10) and isinstance(origin, UnionType):
        origin = UnionType

    return origin


# TODO: remove this once typing_inspect.get_args is fixed for Python 3.10 union types
def get_args(tp: Any) -> tuple[type, ...]:
    """Same as typing_inspect.get_args but fixes Python 3.10 union types."""
    if sys.version_info >= (3, 10) and isinstance(tp, UnionType):
        return tp.__args__

    return typing_inspect_get_args(tp)
