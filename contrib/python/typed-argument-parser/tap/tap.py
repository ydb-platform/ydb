import json
import sys
import time
from argparse import ArgumentParser, ArgumentTypeError
from copy import deepcopy
from functools import partial
from pathlib import Path
from pprint import pformat
from shlex import quote, split
from types import MethodType
from typing import Any, Callable, List, Optional, Sequence, Set, Tuple, TypeVar, Union, get_type_hints
from typing_inspect import is_literal_type

from tap.utils import (
    get_class_variables,
    get_args,
    get_argument_name,
    get_dest,
    get_origin,
    GitInfo,
    is_option_arg,
    is_positional_arg,
    type_to_str,
    get_literals,
    boolean_type,
    TupleTypeEnforcer,
    define_python_object_encoder,
    as_python_object,
    enforce_reproducibility,
    PathLike,
)

if sys.version_info >= (3, 10):
    from types import UnionType


# Constants
EMPTY_TYPE = get_args(List)[0] if len(get_args(List)) > 0 else tuple()
BOXED_COLLECTION_TYPES = {List, list, Set, set, Tuple, tuple}
UNION_TYPES = {Union} | ({UnionType} if sys.version_info >= (3, 10) else set())
OPTIONAL_TYPES = {Optional} | UNION_TYPES
BOXED_TYPES = BOXED_COLLECTION_TYPES | OPTIONAL_TYPES


TapType = TypeVar("TapType", bound="Tap")


class Tap(ArgumentParser):
    """Tap is a typed argument parser that wraps Python's built-in ArgumentParser."""

    def __init__(
        self,
        *args,
        underscores_to_dashes: bool = False,
        explicit_bool: bool = False,
        config_files: Optional[list[PathLike]] = None,
        **kwargs,
    ) -> None:
        """Initializes the Tap instance.

        :param args: Arguments passed to the super class ArgumentParser.
        :param underscores_to_dashes: If True, convert underscores in flags to dashes.
        :param explicit_bool: Booleans can be specified on the command line as "--arg True" or "--arg False"
                              rather than "--arg". Additionally, booleans can be specified by prefixes of True and False
                              with any capitalization as well as 1 or 0.
        :param config_files: A list of paths to configuration files containing the command line arguments
                             (e.g., '--arg1 a1 --arg2 a2'). Arguments passed in from the command line
                             overwrite arguments from the configuration files. Arguments in configuration files
                             that appear later in the list overwrite the arguments in previous configuration files.
        :param kwargs: Keyword arguments passed to the super class ArgumentParser.
        """
        # Whether the Tap object has been initialized
        self._initialized = False

        # Whether boolean flags have to be explicitly set to True or False
        self._explicit_bool = explicit_bool

        # Whether we convert underscores in the flag names to dashes
        self._underscores_to_dashes = underscores_to_dashes

        # Whether the arguments have been parsed (i.e. if parse_args has been called)
        self._parsed = False

        # Set extra arguments to empty list
        self.extra_args = []

        # Create argument buffer
        self.argument_buffer = {}

        # Create a place to put the subparsers
        self._subparser_buffer: list[tuple[str, type, dict[str, Any]]] = []

        # Get class variables help strings from the comments
        self.class_variables = self._get_class_variables()

        # Get annotations from self and all super classes up through tap
        self._annotations = self._get_annotations()

        # Set the default description to be the docstring
        kwargs.setdefault("description", self.__doc__)

        # Initialize the super class, i.e. ArgumentParser
        super(Tap, self).__init__(*args, **kwargs)

        # Stores the subparsers
        self._subparsers = None

        # Load in the configuration files
        self.args_from_configs = self._load_from_config_files(config_files)

        # Perform additional configuration such as adding arguments or adding subparsers
        self._configure()

        # Indicate that initialization is complete
        self._initialized = True

    def _add_argument(self, *name_or_flags, **kwargs) -> None:
        """Adds an argument to self (i.e. the super class ArgumentParser).

        Sets the following attributes of kwargs when not explicitly provided:
        - type: Set to the type annotation of the argument.
        - default: Set to the default value of the argument (if provided).
        - required: True if a default value of the argument is not provided, False otherwise.
        - action: Set to "store_true" if the argument is a required bool or a bool with default value False.
                  Set to "store_false" if the argument is a bool with default value True.
        - nargs: Set to "*" if the type annotation is List[str], List[int], or List[float].
        - help: Set to the argument documentation from the class docstring.

        :param name_or_flags: Either a name or a list of option strings, e.g. foo or -f, --foo.
        :param kwargs: Keyword arguments.
        """
        # Set explicit bool
        explicit_bool = self._explicit_bool

        # Get variable name
        variable = get_argument_name(*name_or_flags)

        if self._underscores_to_dashes:
            variable = variable.replace("-", "_")

        # Get default if not specified
        if hasattr(self, variable):
            kwargs["default"] = kwargs.get("default", getattr(self, variable))

        # Set required if option arg
        if (
            is_option_arg(*name_or_flags)
            and variable != "help"
            and "default" not in kwargs
            and kwargs.get("action") != "version"
        ):
            kwargs["required"] = kwargs.get("required", not hasattr(self, variable))

        # Set help if necessary
        if "help" not in kwargs:
            kwargs["help"] = "("

            # Type
            if variable in self._annotations:
                kwargs["help"] += type_to_str(self._annotations[variable]) + ", "

            # Required/default
            if kwargs.get("required", False) or is_positional_arg(*name_or_flags):
                kwargs["help"] += "required"
            else:
                kwargs["help"] += f'default={kwargs.get("default", None)}'

            kwargs["help"] += ")"

            # Description
            if variable in self.class_variables:
                kwargs["help"] += " " + self.class_variables[variable]["comment"]

        # Set other kwargs where not provided
        if variable in self._annotations:
            # Get type annotation
            var_type = self._annotations[variable]

            # If type is not explicitly provided, set it if it's one of our supported default types
            if "type" not in kwargs:
                # Unbox Union[type] (Optional[type]) and set var_type = type
                if get_origin(var_type) in OPTIONAL_TYPES:
                    var_args = get_args(var_type)

                    # If type is Union or Optional without inner types, set type to equivalent of Optional[str]
                    if len(var_args) == 0:
                        var_args = (str, type(None))

                    # Raise error if type function is not explicitly provided for Union types (not including Optionals)
                    if get_origin(var_type) in UNION_TYPES and not (len(var_args) == 2 and var_args[1] == type(None)):
                        raise ArgumentTypeError(
                            "For Union types, you must include an explicit type function in the configure method. "
                            "For example,\n\n"
                            "def to_number(string: str) -> Union[float, int]:\n"
                            "    return float(string) if '.' in string else int(string)\n\n"
                            "class Args(Tap):\n"
                            "    arg: Union[float, int]\n"
                            "\n"
                            "    def configure(self) -> None:\n"
                            "        self.add_argument('--arg', type=to_number)"
                        )

                    if len(var_args) > 0:
                        var_type = var_args[0]

                        # If var_type is tuple as in Python 3.6, change to a typing type
                        # (e.g., (typing.List, <class 'bool'>) ==> typing.List[bool])
                        if isinstance(var_type, tuple):
                            var_type = var_type[0][var_type[1:]]

                        explicit_bool = True

                # First check whether it is a literal type or a boxed literal type
                if is_literal_type(var_type):
                    var_type, kwargs["choices"] = get_literals(var_type, variable)

                elif (
                    get_origin(var_type) in (List, list, Set, set)
                    and len(get_args(var_type)) > 0
                    and is_literal_type(get_args(var_type)[0])
                ):
                    var_type, kwargs["choices"] = get_literals(get_args(var_type)[0], variable)
                    if kwargs.get("action") not in {"append", "append_const"}:
                        kwargs["nargs"] = kwargs.get("nargs", "*")

                # Handle Tuple type (with type args) by extracting types of Tuple elements and enforcing them
                elif get_origin(var_type) in (Tuple, tuple) and len(get_args(var_type)) > 0:
                    loop = False
                    types = list(get_args(var_type))

                    # Handle Tuple[type, ...]
                    if len(types) == 2 and types[1] == Ellipsis:
                        types = types[0:1]
                        loop = True
                        kwargs["nargs"] = "*"
                    # Handle Tuple[()]
                    elif len(types) == 1 and types[0] == tuple():
                        types = [str]
                        loop = True
                        kwargs["nargs"] = "*"
                    else:
                        kwargs["nargs"] = len(types)

                    # Handle Literal types
                    types = [get_literals(tp, variable)[0] if is_literal_type(tp) else tp for tp in types]

                    var_type = TupleTypeEnforcer(types=types, loop=loop)

                if get_origin(var_type) in BOXED_TYPES:
                    # If List or Set or Tuple type, set nargs
                    if get_origin(var_type) in BOXED_COLLECTION_TYPES and kwargs.get("action") not in {
                        "append",
                        "append_const",
                    }:
                        kwargs["nargs"] = kwargs.get("nargs", "*")

                    # Extract boxed type for Optional, List, Set
                    arg_types = get_args(var_type)

                    # Set defaults type to str for Type and Type[()]
                    if len(arg_types) == 0 or arg_types[0] == EMPTY_TYPE:
                        var_type = str
                    else:
                        var_type = arg_types[0]

                    # Handle the cases of List[bool], Set[bool], Tuple[bool]
                    if var_type == bool:
                        var_type = boolean_type

                # If bool then set action, otherwise set type
                if var_type == bool:
                    if explicit_bool:
                        kwargs["type"] = boolean_type
                        kwargs["choices"] = [True, False]  # this makes the help message more helpful
                    else:
                        action_cond = "true" if kwargs.get("required", False) or not kwargs["default"] else "false"
                        kwargs["action"] = kwargs.get("action", f"store_{action_cond}")
                elif kwargs.get("action") not in {"count", "append_const"}:
                    kwargs["type"] = var_type

        if self._underscores_to_dashes:
            # Replace "_" with "-" for arguments that aren't positional
            name_or_flags = tuple(
                name_or_flag.replace("_", "-") if name_or_flag.startswith("-") else name_or_flag
                for name_or_flag in name_or_flags
            )

        # Deepcopy default to prevent mutation of values
        if "default" in kwargs:
            kwargs["default"] = deepcopy(kwargs["default"])

        super(Tap, self).add_argument(*name_or_flags, **kwargs)

    def add_argument(self, *name_or_flags, **kwargs) -> None:
        """Adds an argument to the argument buffer, which will later be passed to _add_argument."""
        if self._initialized:
            raise ValueError(
                "add_argument cannot be called after initialization. "
                "Arguments must be added either as class variables or by overriding "
                "configure and including a self.add_argument call there."
            )

        variable = get_argument_name(*name_or_flags).replace("-", "_")
        self.argument_buffer[variable] = (name_or_flags, kwargs)

    def _add_arguments(self) -> None:
        """Add arguments to self in the order they are defined as class variables (so the help string is in order)."""
        # Add class variables (in order)
        for variable in self.class_variables:
            if variable in self.argument_buffer:
                name_or_flags, kwargs = self.argument_buffer[variable]
                self._add_argument(*name_or_flags, **kwargs)
            else:
                self._add_argument(f"--{variable}")

        # Add any arguments that were added manually in configure but aren't class variables (in order)
        for variable, (name_or_flags, kwargs) in self.argument_buffer.items():
            if variable not in self.class_variables:
                self._add_argument(*name_or_flags, **kwargs)

    def process_args(self) -> None:
        """Perform additional argument processing and/or validation."""
        pass

    def add_subparser(self, flag: str, subparser_type: type, **kwargs) -> None:
        """Add a subparser to the collection of subparsers"""
        help_desc = kwargs.get("help", subparser_type.__doc__)
        kwargs["help"] = help_desc

        self._subparser_buffer.append((flag, subparser_type, kwargs))

    def _add_subparsers(self) -> None:
        """Add each of the subparsers to the Tap object."""
        # Initialize the _subparsers object if not already created
        if self._subparsers is None and len(self._subparser_buffer) > 0:
            self._subparsers = super(Tap, self).add_subparsers()

        # Load each subparser
        for flag, subparser_type, kwargs in self._subparser_buffer:
            self._subparsers._parser_class = partial(subparser_type, underscores_to_dashes=self._underscores_to_dashes)
            self._subparsers.add_parser(flag, **kwargs)

    def add_subparsers(self, **kwargs) -> None:
        self._subparsers = super().add_subparsers(**kwargs)

    def _configure(self) -> None:
        """Executes the user-defined configuration."""
        # Call the user-defined configuration
        self.configure()

        # Add arguments to self
        self._add_arguments()

        # Add subparsers to self
        self._add_subparsers()

    def configure(self) -> None:
        """Overwrite this method to configure the parser during initialization.

        For example,
            self.add_argument('--sum',
                              dest='accumulate',
                              action='store_const',
                              const=sum,
                              default=max)
            self.add_subparsers(help='sub-command help')
            self.add_subparser('a', SubparserA, help='a help')
        """
        pass

    @staticmethod
    def get_reproducibility_info(repo_path: Optional[PathLike] = None) -> dict[str, str]:
        """Gets a dictionary of reproducibility information.

        Reproducibility information always includes:
        - command_line: The command line command used to execute the code.
        - time: The current time.

        If git is installed, reproducibility information also includes:
        - git_root: The root of the git repo where the command is run.
        - git_url: The url of the current hash of the git repo where the command is run.
                   Ex. https://github.com/swansonk14/rationale-alignment/tree/<hash>.
                   If it is a local repo, the url is None.
        - git_has_uncommitted_changes: Whether the current git repo has uncommitted changes.

        :param repo_path: Path to the git repo to examine for reproducibility info.
                          If None, uses the git repo of the Python file that is run.
        :return: A dictionary of reproducibility information.
        """
        # Get the path to the Python file that is being run
        if repo_path is None:
            repo_path = (Path.cwd() / Path(sys.argv[0]).parent).resolve()

        reproducibility = {
            "command_line": f'python {" ".join(quote(arg) for arg in sys.argv)}',
            "time": time.strftime("%c"),
        }

        git_info = GitInfo(repo_path=repo_path)

        if git_info.has_git():
            reproducibility["git_root"] = git_info.get_git_root()
            reproducibility["git_url"] = git_info.get_git_url(commit_hash=True)
            reproducibility["git_has_uncommitted_changes"] = str(git_info.has_uncommitted_changes())

        return reproducibility

    def _log_all(self, repo_path: Optional[PathLike] = None) -> dict[str, Any]:
        """Gets all arguments along with reproducibility information.

        :param repo_path: Path to the git repo to examine for reproducibility info.
                          If None, uses the git repo of the Python file that is run.
        :return: A dictionary containing all arguments along with reproducibility information.
        """
        arg_log = self.as_dict()
        arg_log["reproducibility"] = self.get_reproducibility_info(repo_path=repo_path)

        return arg_log

    def parse_args(
        self: TapType,
        args: Optional[Sequence[str]] = None,
        known_only: bool = False,
        legacy_config_parsing: bool = False,
    ) -> TapType:
        """Parses arguments, sets attributes of self equal to the parsed arguments, and processes arguments.

        :param args: List of strings to parse. The default is taken from `sys.argv`.
        :param known_only: If true, ignores extra arguments and only parses known arguments.
                           Unparsed arguments are saved to self.extra_args.
        :param legacy_config_parsing: If true, config files are parsed using `str.split` instead of `shlex.split`.
        :return: self, which is a Tap instance containing all of the parsed args.
        """
        # Prevent double parsing
        if self._parsed:
            raise ValueError("parse_args can only be called once.")

        # Collect arguments from all of the configs

        if legacy_config_parsing:
            splitter = lambda arg_string: arg_string.split()
        else:
            splitter = lambda arg_string: split(arg_string, comments=True)

        config_args = [arg for args_from_config in self.args_from_configs for arg in splitter(args_from_config)]

        # Add config args at lower precedence and extract args from the command line if they are not passed explicitly
        args = config_args + (sys.argv[1:] if args is None else list(args))

        # Parse args using super class ArgumentParser's parse_args or parse_known_args function
        if known_only:
            default_namespace, self.extra_args = super(Tap, self).parse_known_args(args)
        else:
            default_namespace = super(Tap, self).parse_args(args)

        # Copy parsed arguments to self
        for variable, value in vars(default_namespace).items():
            # Conversion from list to set or tuple
            if variable in self._annotations:
                if type(value) == list:
                    var_type = get_origin(self._annotations[variable])

                    # Unpack nested boxed types such as Optional[List[int]]
                    if var_type is Union:
                        var_type = get_origin(get_args(self._annotations[variable])[0])

                        # If var_type is tuple as in Python 3.6, change to a typing type
                        # (e.g., (typing.Tuple, <class 'bool'>) ==> typing.Tuple)
                        if isinstance(var_type, tuple):
                            var_type = var_type[0]

                    if var_type in (Set, set):
                        value = set(value)
                    elif var_type in (Tuple, tuple):
                        value = tuple(value)

            # Set variable in self
            setattr(self, variable, value)

        # Process args
        self.process_args()

        # Indicate that args have been parsed
        self._parsed = True

        return self

    @classmethod
    def _get_from_self_and_super(cls, extract_func: Callable[[type], dict]) -> Union[dict[str, Any], dict]:
        """Returns a dictionary mapping variable names to values.

        Variables and values are extracted from classes using key starting
        with this class and traversing up the super classes up through Tap.

        If super class and subclass have the same key, the subclass value is used.

        Super classes are traversed through breadth first search.

        :param extract_func: A function that extracts from a class a dictionary mapping variables to values.
        :return: A dictionary mapping variable names to values from the class dict.
        """
        visited = set()
        super_classes = [cls]
        dictionary = {}

        while len(super_classes) > 0:
            super_class = super_classes.pop(0)

            if super_class not in visited and issubclass(super_class, Tap) and super_class is not Tap:
                super_dictionary = extract_func(super_class)

                # Update only unseen variables to avoid overriding subclass values
                for variable, value in super_dictionary.items():
                    if variable not in dictionary:
                        dictionary[variable] = value
                for variable in super_dictionary.keys() - dictionary.keys():
                    dictionary[variable] = super_dictionary[variable]

                super_classes += list(super_class.__bases__)
                visited.add(super_class)

        return dictionary

    def _get_class_dict(self) -> dict[str, Any]:
        """Returns a dictionary mapping class variable names to values from the class dict."""
        class_dict = self._get_from_self_and_super(
            extract_func=lambda super_class: dict(getattr(super_class, "__dict__", dict()))
        )
        class_dict = {
            var: val
            for var, val in class_dict.items()
            if not (var.startswith("_") or callable(val) or isinstance(val, (staticmethod, classmethod, property)))
        }

        return class_dict

    def _get_annotations(self) -> dict[str, Any]:
        """Returns a dictionary mapping variable names to their type annotations."""
        return self._get_from_self_and_super(extract_func=lambda super_class: dict(get_type_hints(super_class)))

    def _get_class_variables(self) -> dict:
        """Returns a dictionary mapping class variables names to their additional information."""
        class_variable_names = {**self._get_annotations(), **self._get_class_dict()}.keys()

        try:
            class_variables = self._get_from_self_and_super(extract_func=get_class_variables)

            # Handle edge-case of source code modification while code is running
            variables_to_add = (
                variable for variable in class_variable_names if variable not in class_variables
            )
            variables_to_remove = (
                variable for variable in class_variables.keys() if variable not in class_variable_names
            )

            for variable in variables_to_add:
                class_variables[variable] = {"comment": ""}

            for variable in variables_to_remove:
                class_variables.pop(variable)
        # Exception if inspect.getsource fails to extract the source code
        except Exception:
            class_variables = {}
            for variable in class_variable_names:
                class_variables[variable] = {"comment": ""}

        return class_variables

    def _get_argument_names(self) -> set[str]:
        """Returns a list of variable names corresponding to the arguments."""
        return (
            {get_dest(*name_or_flags, **kwargs) for name_or_flags, kwargs in self.argument_buffer.values()}
            | set(self._get_class_dict().keys())
            | set(self._annotations.keys())
        ) - {"help"}

    def as_dict(self) -> dict[str, Any]:
        """Returns the member variables corresponding to the parsed arguments.

        Note: This does not include attributes set directly on an instance
        (e.g. arg is not included in MyTap().arg = "hi")

        :return: A dictionary mapping each argument's name to its value.
        """
        if not self._parsed:
            raise ValueError("You should call `parse_args` before retrieving arguments.")

        self_dict = self.__dict__
        class_dict = self._get_from_self_and_super(
            extract_func=lambda super_class: dict(getattr(super_class, "__dict__", dict()))
        )
        class_dict = {key: val for key, val in class_dict.items() if key not in self_dict}
        stored_dict = {**self_dict, **class_dict}

        stored_dict = {
            var: getattr(self, var)
            for var, val in stored_dict.items()
            if not (var.startswith("_") or isinstance(val, MethodType) or isinstance(val, staticmethod))
        }

        tap_class_dict_keys = Tap().__dict__.keys() | Tap.__dict__.keys()
        stored_dict = {key: stored_dict[key] for key in stored_dict.keys() - tap_class_dict_keys}

        return stored_dict

    def from_dict(self, args_dict: dict[str, Any], skip_unsettable: bool = False) -> TapType:
        """Loads arguments from a dictionary, ensuring all required arguments are set.

        :param args_dict: A dictionary from argument names to the values of the arguments.
        :param skip_unsettable: When True, skips attributes that cannot be set in the Tap object,
                                e.g. properties without setters.
        :return: Returns self.
        """
        # All of the required arguments must be provided or already set
        required_args = {a.dest for a in self._actions if a.required}
        unprovided_required_args = required_args - args_dict.keys()
        missing_required_args = [arg for arg in unprovided_required_args if not hasattr(self, arg)]

        if len(missing_required_args) > 0:
            raise ValueError(
                f'Input dictionary "{args_dict}" does not include '
                f'all unset required arguments: "{missing_required_args}".'
            )

        # Load all arguments
        for key, value in args_dict.items():
            try:
                setattr(self, key, value)
            except AttributeError:
                if not skip_unsettable:
                    raise AttributeError(
                        f'Cannot set attribute "{key}" to "{value}". '
                        f"To skip arguments that cannot be set \n"
                        f'\t"skip_unsettable = True"'
                    )

        self._parsed = True

        return self

    def save(
        self,
        path: PathLike,
        with_reproducibility: bool = True,
        skip_unpicklable: bool = False,
        repo_path: Optional[PathLike] = None,
    ) -> None:
        """Saves the arguments and reproducibility information in JSON format, pickling what can't be encoded.

        :param path: Path to the JSON file where the arguments will be saved.
        :param with_reproducibility: If True, adds a "reproducibility" field with information (e.g. git hash)
                                     to the JSON file.
        :param repo_path: Path to the git repo to examine for reproducibility info.
                          If None, uses the git repo of the Python file that is run.
        :param skip_unpicklable: If True, does not save attributes whose values cannot be pickled.
        """
        with open(path, "w") as f:
            args = self._log_all(repo_path=repo_path) if with_reproducibility else self.as_dict()
            json.dump(args, f, indent=4, sort_keys=True, cls=define_python_object_encoder(skip_unpicklable))

    def load(
        self,
        path: PathLike,
        check_reproducibility: bool = False,
        skip_unsettable: bool = False,
        repo_path: Optional[PathLike] = None,
    ) -> TapType:
        """Loads the arguments in JSON format. Note: Due to JSON, tuples are loaded as lists.

        :param path: Path to the JSON file where the arguments will be loaded from.
        :param check_reproducibility: When True, raises an error if the loaded reproducibility
                                      information doesn't match the current reproducibility information.
        :param skip_unsettable: When True, skips attributes that cannot be set in the Tap object,
                                e.g. properties without setters.
        :param repo_path: Path to the git repo to examine for reproducibility info.
                          If None, uses the git repo of the Python file that is run.
        :return: Returns self.
        """
        with open(path) as f:
            args_dict = json.load(f, object_hook=as_python_object)

        # Remove loaded reproducibility information since it is no longer valid
        saved_reproducibility_data = args_dict.pop("reproducibility", None)
        if check_reproducibility:
            current_reproducibility_data = self.get_reproducibility_info(repo_path=repo_path)
            enforce_reproducibility(saved_reproducibility_data, current_reproducibility_data, path)

        self.from_dict(args_dict, skip_unsettable=skip_unsettable)

        return self

    def _load_from_config_files(self, config_files: Optional[list[str]]) -> list[str]:
        """Loads arguments from a list of configuration files containing command line arguments.

        :param config_files: A list of paths to configuration files containing the command line arguments
                             (e.g., '--arg1 a1 --arg2 a2'). Arguments passed in from the command line
                             overwrite arguments from the configuration files. Arguments in configuration files
                             that appear later in the list overwrite the arguments in previous configuration files.
        :return: A list of the contents of each config file in order of increasing precedence (highest last).
        """
        args_from_config = []

        if config_files is not None:
            # Read arguments from all configs from the lowest precedence config to the highest
            for file in config_files:
                with open(file) as f:
                    args_from_config.append(f.read().strip())

        return args_from_config

    def __str__(self) -> str:
        """Returns a string representation of self.

        :return: A formatted string representation of the dictionary of all arguments.
        """
        return pformat(self.as_dict())

    def __deepcopy__(self, memo: dict[int, Any] = None) -> TapType:
        """Deepcopy the Tap object."""
        copied = type(self).__new__(type(self))

        if memo is None:
            memo = {}

        memo[id(self)] = copied

        for k, v in self.__dict__.items():
            copied.__dict__[k] = deepcopy(v, memo)

        return copied

    def __getstate__(self) -> dict[str, Any]:
        """Gets the state of the object for pickling."""
        return self.as_dict()

    def __setstate__(self, d: dict[str, Any]) -> None:
        """
        Initializes the object with the provided dictionary of arguments for unpickling.

        :param d: A dictionary of arguments.
        """
        self.__init__()
        self.from_dict(d)
