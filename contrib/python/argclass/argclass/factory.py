"""Factory functions for creating arguments."""

from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Literal,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from argparse import Action

from .exceptions import EnumValueError
from .store import ConfigArgument, INIConfig, TypedArgument
from .types import (
    Actions,
    ConverterType,
    LogLevelEnum,
    MetavarType,
    Nargs,
    NargsType,
)

T = TypeVar("T")


# noinspection PyShadowingBuiltins
def ArgumentSingle(
    *aliases: str,
    type: Type[T],
    action: Union[Actions, Type[Action]] = Actions.default(),
    choices: Optional[Iterable[str]] = None,
    const: Optional[Any] = None,
    converter: Optional[Callable[[T], T]] = None,
    default: Optional[T] = None,
    env_var: Optional[str] = None,
    help: Optional[str] = None,
    metavar: Optional[MetavarType] = None,
    required: Optional[bool] = None,
    secret: bool = False,
) -> T:
    """
    Create a single-value argument with precise typing.

    Use this when you need exact type inference for a single value.
    The `type` parameter is required and determines the return type.

    Example:
        class Parser(argclass.Parser):
            count: int = argclass.ArgumentSingle(type=int, default=10)
            name: str = argclass.ArgumentSingle(type=str)
    """
    return cast(
        T,
        TypedArgument(
            action=action,
            aliases=aliases,
            choices=choices,
            const=const,
            converter=converter,
            default=default,
            secret=secret,
            env_var=env_var,
            help=help,
            metavar=metavar,
            nargs=None,
            required=required,
            type=type,
        ),
    )


# noinspection PyShadowingBuiltins
def ArgumentSequence(
    *aliases: str,
    type: Type[T],
    nargs: NargsType = Nargs.ONE_OR_MORE,
    action: Union[Actions, Type[Action]] = Actions.default(),
    choices: Optional[Iterable[str]] = None,
    const: Optional[Any] = None,
    converter: Optional[Callable[[List[T]], Any]] = None,
    default: Optional[List[T]] = None,
    env_var: Optional[str] = None,
    help: Optional[str] = None,
    metavar: Optional[MetavarType] = None,
    required: Optional[bool] = None,
    secret: bool = False,
) -> List[T]:
    """
    Create a multi-value argument with precise typing.

    Use this when you need exact type inference for a list of values.
    The ``type`` parameter is required and determines the element type.

    Args:
        nargs: Number of values. Defaults to "+" (one or more).
               Use "*" for zero or more, or an int for exact count.

    Example::

        class Parser(argclass.Parser):
            files: list[str] = argclass.ArgumentSequence(type=str)
            numbers: list[int] = argclass.ArgumentSequence(
                type=int, nargs="*", default=[]
            )
    """
    return cast(
        List[T],
        TypedArgument(
            action=action,
            aliases=aliases,
            choices=choices,
            const=const,
            converter=converter,
            default=default,
            secret=secret,
            env_var=env_var,
            help=help,
            metavar=metavar,
            nargs=nargs,
            required=required,
            type=type,
        ),
    )


# Overload: type + nargs + converter → converter's return type (must be first!)
R = TypeVar("R")


@overload
def Argument(
    *aliases: str,
    type: ConverterType,
    nargs: NargsType,
    converter: Callable[..., R],
    action: Union[Actions, Type[Action]] = ...,
    choices: Optional[Iterable[str]] = ...,
    const: Optional[Any] = ...,
    default: Optional[Any] = ...,
    env_var: Optional[str] = ...,
    help: Optional[str] = ...,
    metavar: Optional[MetavarType] = ...,
    required: Optional[bool] = ...,
    secret: bool = ...,
) -> R: ...


# Overload: Type[T] + nargs="?" (optional single) → T
@overload
def Argument(
    *aliases: str,
    type: Type[T],
    nargs: Literal["?"],
    action: Union[Actions, Type[Action]] = ...,
    choices: Optional[Iterable[str]] = ...,
    const: Optional[Any] = ...,
    converter: None = ...,
    default: Optional[Any] = ...,
    env_var: Optional[str] = ...,
    help: Optional[str] = ...,
    metavar: Optional[MetavarType] = ...,
    required: Optional[bool] = ...,
    secret: bool = ...,
) -> T: ...


# Overload: Callable type + nargs="?" (optional single) → T
@overload
def Argument(
    *aliases: str,
    type: Callable[[str], T],
    nargs: Literal["?"],
    action: Union[Actions, Type[Action]] = ...,
    choices: Optional[Iterable[str]] = ...,
    const: Optional[Any] = ...,
    converter: None = ...,
    default: Optional[Any] = ...,
    env_var: Optional[str] = ...,
    help: Optional[str] = ...,
    metavar: Optional[MetavarType] = ...,
    required: Optional[bool] = ...,
    secret: bool = ...,
) -> T: ...


# Overload: Type[T] + nargs (sequence) → List[T]
@overload
def Argument(
    *aliases: str,
    type: Type[T],
    nargs: Union[Literal["*", "+"], int, Nargs],
    action: Union[Actions, Type[Action]] = ...,
    choices: Optional[Iterable[str]] = ...,
    const: Optional[Any] = ...,
    converter: None = ...,
    default: Optional[Any] = ...,
    env_var: Optional[str] = ...,
    help: Optional[str] = ...,
    metavar: Optional[MetavarType] = ...,
    required: Optional[bool] = ...,
    secret: bool = ...,
) -> List[T]: ...


# Overload: Callable type + nargs (sequence) → List[T]
@overload
def Argument(
    *aliases: str,
    type: Callable[[str], T],
    nargs: Union[Literal["*", "+"], int, Nargs],
    action: Union[Actions, Type[Action]] = ...,
    choices: Optional[Iterable[str]] = ...,
    const: Optional[Any] = ...,
    converter: None = ...,
    default: Optional[Any] = ...,
    env_var: Optional[str] = ...,
    help: Optional[str] = ...,
    metavar: Optional[MetavarType] = ...,
    required: Optional[bool] = ...,
    secret: bool = ...,
) -> List[T]: ...


# Overload: Type[T] without nargs (single) → T
@overload
def Argument(
    *aliases: str,
    type: Type[T],
    action: Union[Actions, Type[Action]] = ...,
    choices: Optional[Iterable[str]] = ...,
    const: Optional[Any] = ...,
    converter: None = ...,
    default: Optional[T] = ...,
    env_var: Optional[str] = ...,
    help: Optional[str] = ...,
    metavar: Optional[MetavarType] = ...,
    nargs: None = ...,
    required: Optional[bool] = ...,
    secret: bool = ...,
) -> T: ...


# Overload: Callable type without nargs (single) → T
@overload
def Argument(
    *aliases: str,
    type: Callable[[str], T],
    action: Union[Actions, Type[Action]] = ...,
    choices: Optional[Iterable[str]] = ...,
    const: Optional[Any] = ...,
    converter: None = ...,
    default: Optional[T] = ...,
    env_var: Optional[str] = ...,
    help: Optional[str] = ...,
    metavar: Optional[MetavarType] = ...,
    nargs: None = ...,
    required: Optional[bool] = ...,
    secret: bool = ...,
) -> T: ...


# Overload: converter determines return type
@overload
def Argument(
    *aliases: str,
    converter: Callable[..., T],
    action: Union[Actions, Type[Action]] = ...,
    choices: Optional[Iterable[str]] = ...,
    const: Optional[Any] = ...,
    default: Optional[Any] = ...,
    env_var: Optional[str] = ...,
    help: Optional[str] = ...,
    metavar: Optional[MetavarType] = ...,
    nargs: Optional[NargsType] = ...,
    required: Optional[bool] = ...,
    secret: bool = ...,
    type: Optional[ConverterType] = ...,
) -> T: ...


# Overload: fallback for dynamic/optional parameters (used by Secret, etc.)
@overload
def Argument(
    *aliases: str,
    action: Union[Actions, Type[Action]] = ...,
    choices: Optional[Iterable[str]] = ...,
    const: Optional[Any] = ...,
    converter: Optional[ConverterType] = ...,
    default: Optional[Any] = ...,
    env_var: Optional[str] = ...,
    help: Optional[str] = ...,
    metavar: Optional[MetavarType] = ...,
    nargs: Optional[NargsType] = ...,
    required: Optional[bool] = ...,
    secret: bool = ...,
    type: Optional[ConverterType] = ...,
) -> Any: ...


# noinspection PyShadowingBuiltins
def Argument(
    *aliases: str,
    action: Union[Actions, Type[Action]] = Actions.default(),
    choices: Optional[Iterable[str]] = None,
    const: Optional[Any] = None,
    converter: Optional[ConverterType] = None,
    default: Optional[Any] = None,
    env_var: Optional[str] = None,
    help: Optional[str] = None,
    metavar: Optional[MetavarType] = None,
    nargs: Optional[NargsType] = None,
    required: Optional[bool] = None,
    secret: bool = False,
    type: Optional[ConverterType] = None,
) -> Any:
    """
    Create a typed argument for a Parser or Group class.

    Dispatches to ArgumentSingle or ArgumentSequence based on nargs.

    Args:
        aliases: Command-line aliases (e.g., "-n", "--name").
        action: How to handle the argument (store, store_true, etc.).
        choices: Restrict values to these options.
        const: Constant value for store_const/append_const actions.
        converter: Post-parse transform function. With nargs, receives the list.
        default: Default value if argument not provided.
        env_var: Environment variable to read default from.
        help: Help text for --help output.
        metavar: Placeholder name in help text.
        nargs: Number of values (int, "?", "*", "+", or Nargs enum).
        required: Whether the argument must be provided.
        secret: If True, hide value from help and wrap str in SecretString.
        type: Per-value converter for argparse. With nargs, called per value.

    Returns:
        TypedArgument instance.

    Example::

        # type: converts each CLI value individually
        numbers = Argument(nargs="+", type=int)
        # Parsing ["1", "2"] -> calls int("1"), int("2") -> [1, 2]

        # converter: transforms the final result after type conversion
        unique = Argument(nargs="+", type=int, converter=set)
        # Parsing ["1", "2", "1"] -> [1, 2, 1] -> set([1, 2, 1]) -> {1, 2}

        # Combining type and converter for set[int]:
        class Parser(argclass.Parser):
            numbers: set[int] = Argument(
                type=int,       # Convert each "1", "2" to int
                converter=set,  # Convert [1, 2, 1] to {1, 2}
                nargs="+",
            )

        # Alternative: single converter function for set[int]:
        class Parser(argclass.Parser):
            numbers: set[int] = Argument(
                converter=lambda vals: set(map(int, vals)),
                nargs="+",
            )
    """
    # Dispatch to typed functions when type is provided
    if type is not None:
        # Cast type to Type[Any] since we've verified it's not None
        # The actual type could be Type[T] or Callable[[str], T]
        type_func = cast(Type[Any], type)
        if nargs in ("+", "*") or isinstance(nargs, (int, Nargs)):
            return ArgumentSequence(
                *aliases,
                type=type_func,
                nargs=nargs,
                action=action,
                choices=choices,
                const=const,
                converter=cast(Optional[Callable[[List[Any]], Any]], converter),
                default=default,
                env_var=env_var,
                help=help,
                metavar=metavar,
                required=required,
                secret=secret,
            )
        elif nargs == "?" or nargs == Nargs.ZERO_OR_ONE:
            # nargs="?" needs special handling - creates TypedArgument directly
            return TypedArgument(
                action=action,
                aliases=aliases,
                choices=choices,
                const=const,
                converter=converter,
                default=default,
                env_var=env_var,
                help=help,
                metavar=metavar,
                nargs=nargs,
                required=required,
                secret=secret,
                type=type,
            )
        else:
            return ArgumentSingle(
                *aliases,
                type=type_func,
                action=action,
                choices=choices,
                const=const,
                converter=converter,
                default=default,
                env_var=env_var,
                help=help,
                metavar=metavar,
                required=required,
                secret=secret,
            )

    # Fallback for untyped arguments
    return TypedArgument(
        action=action,
        aliases=aliases,
        choices=choices,
        const=const,
        converter=converter,
        default=default,
        secret=secret,
        env_var=env_var,
        help=help,
        metavar=metavar,
        nargs=nargs,
        required=required,
        type=type,
    )


E = TypeVar("E", bound=Enum)


# Overload: with default value (enum member or string) -> returns EnumType
@overload
def EnumArgument(
    enum_class: Type[E],
    *aliases: str,
    default: Union[E, str],
    action: Union[Actions, Type[Action]] = ...,
    env_var: Optional[str] = ...,
    help: Optional[str] = ...,
    metavar: Optional[str] = ...,
    nargs: Optional[NargsType] = ...,
    required: Optional[bool] = ...,
    use_value: bool = ...,
    lowercase: bool = ...,
) -> E: ...


# Overload: without default value -> returns Optional[EnumType]
@overload
def EnumArgument(
    enum_class: Type[E],
    *aliases: str,
    action: Union[Actions, Type[Action]] = ...,
    default: None = ...,
    env_var: Optional[str] = ...,
    help: Optional[str] = ...,
    metavar: Optional[str] = ...,
    nargs: Optional[NargsType] = ...,
    required: Optional[bool] = ...,
    use_value: bool = ...,
    lowercase: bool = ...,
) -> Optional[E]: ...


def EnumArgument(
    enum_class: Type[E],
    *aliases: str,
    action: Union[Actions, Type[Action]] = Actions.default(),
    default: Union[E, str, None] = None,
    env_var: Optional[str] = None,
    help: Optional[str] = None,
    metavar: Optional[str] = None,
    nargs: Optional[NargsType] = None,
    required: Optional[bool] = None,
    use_value: bool = False,
    lowercase: bool = False,
) -> Any:
    """
    Create an argument from an Enum class.

    Args:
        enum_class: The Enum class to use for choices and conversion.
        aliases: Command-line aliases (e.g., "-l", "--level").
        action: How to handle the argument.
        default: Default value as enum member or string name.
        env_var: Environment variable to read default from.
        help: Help text for --help output.
        metavar: Placeholder name in help text.
        nargs: Number of values.
        required: Whether the argument must be provided.
        use_value: If True, return enum.value instead of enum member.
        lowercase: If True, use lowercase choices and accept lowercase input.

    Returns:
        TypedArgument instance.
    """
    if lowercase:
        choices = tuple(e.name.lower() for e in enum_class)
    else:
        choices = tuple(e.name for e in enum_class)

    if default is not None:
        if isinstance(default, enum_class):
            pass  # Valid enum member
        elif isinstance(default, str):
            # Validate string is a valid enum member name
            check_name = default.upper() if lowercase else default
            valid_names = tuple(e.name for e in enum_class)
            if check_name not in valid_names:
                raise EnumValueError(
                    f"default {default!r} is not a valid {enum_class.__name__} "
                    f"member",
                    enum_class=enum_class,
                    valid_values=valid_names,
                )
            # Convert string default to enum member
            default = enum_class[check_name]
        else:
            raise EnumValueError(
                f"default must be {enum_class.__name__} member or string, "
                f"got {type(default).__name__}",
                enum_class=enum_class,
                valid_values=tuple(e.name for e in enum_class),
                hint="Pass an enum member or its string name",
            )

    def converter(x: Any) -> Any:
        # Handle None - return as-is for optional arguments
        if x is None:
            return None
        # Handle existing enum members
        if isinstance(x, enum_class):
            return x.value if use_value else x
        # Convert string to enum
        name = x.upper() if lowercase else x
        member = enum_class[name]
        return member.value if use_value else member

    return TypedArgument(
        action=action,
        aliases=aliases,
        choices=choices,
        converter=converter,
        default=default,
        env_var=env_var,
        help=help,
        metavar=metavar,
        nargs=nargs,
        required=required,
    )


# noinspection PyShadowingBuiltins
def Secret(
    *aliases: str,
    action: Union[Actions, Type[Action]] = Actions.default(),
    choices: Optional[Iterable[str]] = None,
    const: Optional[Any] = None,
    converter: Optional[ConverterType] = None,
    default: Optional[Any] = None,
    env_var: Optional[str] = None,
    help: Optional[str] = None,
    metavar: Optional[str] = None,
    nargs: Optional[NargsType] = None,
    required: Optional[bool] = None,
    type: Optional[ConverterType] = None,
) -> Any:
    """
    Create a secret argument that masks sensitive values.

    Secret arguments are wrapped in SecretString, which:

    - Returns ``'******'`` for repr() to prevent accidental logging
    - Supports equality comparison without exposing the value
    - Use str() to access the actual value when needed

    Use ``parser.sanitize_env()`` after parsing to remove secret
    environment variables before spawning subprocesses.

    Args:
        aliases: Command-line aliases (e.g., "-p", "--password").
        env_var: Environment variable to read the secret from.
        default: Default value if not provided.
        help: Help text (the actual value is never shown).

    Returns:
        TypedArgument with secret=True.

    Example::

        class Parser(argclass.Parser):
            api_key: str = argclass.Secret(env_var="API_KEY")
            password: str = argclass.Secret()

        parser = Parser()
        parser.parse_args()
        parser.sanitize_env()  # Remove secrets from environment

        # Safe: shows '******'
        print(f"API key: {parser.api_key!r}")

        # Access actual value
        connect(api_key=str(parser.api_key))
    """
    return Argument(
        *aliases,
        action=action,
        choices=choices,
        const=const,
        converter=converter,
        default=default,
        env_var=env_var,
        help=help,
        metavar=metavar,
        nargs=nargs,
        required=required,
        secret=True,
        type=type,
    )


# noinspection PyShadowingBuiltins
def Config(
    *aliases: str,
    search_paths: Optional[Iterable[Union[Path, str]]] = None,
    choices: Optional[Iterable[str]] = None,
    converter: Optional[ConverterType] = None,
    const: Optional[Any] = None,
    default: Optional[Any] = None,
    env_var: Optional[str] = None,
    help: Optional[str] = None,
    metavar: Optional[str] = None,
    nargs: Optional[NargsType] = None,
    required: Optional[bool] = None,
    config_class: Type[ConfigArgument] = INIConfig,
) -> Any:
    """
    Create a configuration file argument.

    This creates a ``--config`` argument that loads structured data
    from a file. The loaded data is accessible as a dict-like object.

    Note:
        This is different from ``config_files`` parameter on Parser,
        which presets CLI argument defaults. This argument loads
        arbitrary configuration data for your application.

    Args:
        aliases: Command-line aliases (default: "--config").
        search_paths: Default paths to search for config files.
        config_class: Parser class (INIConfig, JSONConfig, TOMLConfig).
        env_var: Environment variable for config file path.
        help: Help text for --help output.

    Returns:
        ConfigArgument instance.

    Example::

        class Parser(argclass.Parser):
            config = argclass.Config(config_class=argclass.JSONConfig)

        parser = Parser()
        parser.parse_args(["--config", "settings.json"])

        # Access loaded configuration
        db_host = parser.config["database"]["host"]
    """
    return config_class(
        search_paths=search_paths,
        aliases=aliases,
        choices=choices,
        converter=converter,
        const=const,
        default=default,
        env_var=env_var,
        help=help,
        metavar=metavar,
        nargs=nargs,
        required=required,
    )


# Pre-built log level argument
LogLevel = EnumArgument(
    LogLevelEnum,
    use_value=True,
    lowercase=True,
    default=LogLevelEnum.INFO,
)
