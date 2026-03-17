"""
`tapify`: initialize a class or run a function by parsing arguments from the command line.

`to_tap_class`: convert a class or function into a `Tap` class, which can then be subclassed to add special argument
handling
"""

import dataclasses
import inspect
from typing import Any, Callable, Optional, Sequence, TypeVar, Union

from docstring_parser import Docstring, parse

try:
    import pydantic
except ModuleNotFoundError:
    _IS_PYDANTIC_V1 = None
    # These are "empty" types. isinstance and issubclass will always be False
    BaseModel = type("BaseModel", (object,), {})
    _PydanticField = type("_PydanticField", (object,), {})
    _PYDANTIC_FIELD_TYPES = ()
else:
    _IS_PYDANTIC_V1 = pydantic.VERSION.startswith("1.")
    from pydantic import BaseModel
    from pydantic.fields import FieldInfo as PydanticFieldBaseModel
    from pydantic.dataclasses import FieldInfo as PydanticFieldDataclass

    _PydanticField = Union[PydanticFieldBaseModel, PydanticFieldDataclass]
    # typing.get_args(_PydanticField) is an empty tuple for some reason. Just repeat
    _PYDANTIC_FIELD_TYPES = (PydanticFieldBaseModel, PydanticFieldDataclass)

from tap import Tap

OutputType = TypeVar("OutputType")

_ClassOrFunction = Union[Callable[..., OutputType], type[OutputType]]


@dataclasses.dataclass
class _ArgData:
    """
    Data about an argument which is sufficient to inform a Tap variable/argument.
    """

    name: str

    annotation: type
    "The type of values this argument accepts"

    is_required: bool
    "Whether or not the argument must be passed in"

    default: Any
    "Value of the argument if the argument isn't passed in. This gets ignored if `is_required`"

    description: Optional[str] = ""
    "Human-readable description of the argument"

    is_positional_only: bool = False
    "Whether or not the argument must be provided positionally"


@dataclasses.dataclass(frozen=True)
class _TapData:
    """
    Data about a class' or function's arguments which are sufficient to inform a Tap class.
    """

    args_data: list[_ArgData]
    "List of data about each argument in the class or function"

    has_kwargs: bool
    "True if you can pass variable/extra kwargs to the class or function (as in **kwargs), else False"

    known_only: bool
    "If true, ignore extra arguments and only parse known arguments"


def _is_pydantic_base_model(obj: Union[type[Any], Any]) -> bool:
    if inspect.isclass(obj):  # issubclass requires that obj is a class
        return issubclass(obj, BaseModel)
    else:
        return isinstance(obj, BaseModel)


def _is_pydantic_dataclass(obj: Union[type[Any], Any]) -> bool:
    if _IS_PYDANTIC_V1:
        # There's no public function in v1. This is a somewhat safe but linear check
        return dataclasses.is_dataclass(obj) and any(key.startswith("__pydantic") for key in obj.__dict__)
    else:
        return pydantic.dataclasses.is_pydantic_dataclass(obj)


def _tap_data_from_data_model(
    data_model: Any, func_kwargs: dict[str, Any], param_to_description: dict[str, str] = None
) -> _TapData:
    """
    Currently only works when `data_model` is a:
      - builtin dataclass (class or instance)
      - Pydantic dataclass (class or instance)
      - Pydantic BaseModel (class or instance).

    The advantage of this function over :func:`_tap_data_from_class_or_function` is that field/argument descriptions are
    extracted, b/c this function look at the fields of the data model.

    Note
    ----
    Deletes redundant keys from `func_kwargs`
    """
    param_to_description = param_to_description or {}

    def arg_data_from_dataclass(name: str, field: dataclasses.Field) -> _ArgData:
        def is_required(field: dataclasses.Field) -> bool:
            return field.default is dataclasses.MISSING and field.default_factory is dataclasses.MISSING

        description = param_to_description.get(name, field.metadata.get("description"))
        return _ArgData(
            name,
            field.type,
            is_required(field),
            field.default,
            description,
        )

    def arg_data_from_pydantic(name: str, field: _PydanticField, annotation: Optional[type] = None) -> _ArgData:
        annotation = field.annotation if annotation is None else annotation
        # Prefer the description from param_to_description (from the data model / class docstring) over the
        # field.description b/c a docstring can be modified on the fly w/o causing real issues
        description = param_to_description.get(name, field.description)
        return _ArgData(name, annotation, field.is_required(), field.default, description)

    # Determine what type of data model it is and extract fields accordingly
    if dataclasses.is_dataclass(data_model):
        name_to_field = {field.name: field for field in dataclasses.fields(data_model)}
        has_kwargs = False
        known_only = False
    elif _is_pydantic_base_model(data_model):
        name_to_field = data_model.model_fields
        # For backwards compatibility, only allow new kwargs to get assigned if the model is explicitly configured to do
        # so via extra="allow". See https://docs.pydantic.dev/latest/api/config/#pydantic.config.ConfigDict.extra
        is_extra_ok = data_model.model_config.get("extra", "ignore") == "allow"
        has_kwargs = is_extra_ok
        known_only = is_extra_ok
    else:
        raise TypeError(
            "data_model must be a builtin or Pydantic dataclass (instance or class) or "
            f"a Pydantic BaseModel (instance or class). Got {type(data_model)}"
        )

    # It's possible to mix fields w/ classes, e.g., use pydantic Fields in a (builtin) dataclass, or use (builtin)
    # dataclass fields in a pydantic BaseModel. It's also possible to use (builtin) dataclass fields and pydantic Fields
    # in the same data model. Therefore, the type of the data model doesn't determine the type of each field. The
    # solution is to iterate through the fields and check each type.
    args_data: list[_ArgData] = []
    for name, field in name_to_field.items():
        if isinstance(field, dataclasses.Field):
            # Idiosyncrasy: if a pydantic Field is used in a pydantic dataclass, then field.default is a FieldInfo
            # object instead of the field's default value. Furthermore, field.annotation is always NoneType. Luckily,
            # the actual type of the field is stored in field.type
            if isinstance(field.default, _PYDANTIC_FIELD_TYPES):
                arg_data = arg_data_from_pydantic(name, field.default, annotation=field.type)
            else:
                arg_data = arg_data_from_dataclass(name, field)
        elif isinstance(field, _PYDANTIC_FIELD_TYPES):
            arg_data = arg_data_from_pydantic(name, field)
        else:
            raise TypeError(f"Each field must be a dataclass or Pydantic field. Got {type(field)}")
        # Handle case where func_kwargs is supplied
        if name in func_kwargs:
            arg_data.default = func_kwargs[name]
            arg_data.is_required = False
            del func_kwargs[name]
        args_data.append(arg_data)
    return _TapData(args_data, has_kwargs, known_only)


def _tap_data_from_class_or_function(
    class_or_function: _ClassOrFunction, func_kwargs: dict[str, Any], param_to_description: dict[str, str]
) -> _TapData:
    """
    Extract data by inspecting the signature of `class_or_function`.

    Note
    ----
    Deletes redundant keys from `func_kwargs`
    """
    args_data: list[_ArgData] = []
    has_kwargs = False
    known_only = False

    sig = inspect.signature(class_or_function)

    is_pydantic_v1_dataclass_class = (
        _IS_PYDANTIC_V1 and _is_pydantic_dataclass(class_or_function) and inspect.isclass(class_or_function)
    )

    for idx, (param_name, param) in enumerate(sig.parameters.items()):
        if (
            idx == 0
            and param.annotation == inspect.Parameter.empty
            and param_name == "self"
            and is_pydantic_v1_dataclass_class
        ):
            # This check gets around a quirk of Pydantic v1 dataclasses. Sometime after Python 3.13.0, the signature
            # started including self as the first parameter out of inspect.signature
            continue

        # Skip **kwargs
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            has_kwargs = True
            known_only = True
            continue

        if param.annotation != inspect.Parameter.empty:
            annotation = param.annotation
        else:
            annotation = Any

        if param.name in func_kwargs:
            is_required = False
            default = func_kwargs[param.name]
            del func_kwargs[param.name]
        elif param.default != inspect.Parameter.empty:
            is_required = False
            default = param.default
        else:
            is_required = True
            default = inspect.Parameter.empty  # Can be set to anything. It'll be ignored

        arg_data = _ArgData(
            name=param_name,
            annotation=annotation,
            is_required=is_required,
            default=default,
            description=param_to_description.get(param.name),
            is_positional_only=param.kind == inspect.Parameter.POSITIONAL_ONLY,
        )
        args_data.append(arg_data)
    return _TapData(args_data, has_kwargs, known_only)


def _is_data_model(obj: Union[type[Any], Any]) -> bool:
    return dataclasses.is_dataclass(obj) or _is_pydantic_base_model(obj)


def _docstring(class_or_function) -> Docstring:
    is_function = not inspect.isclass(class_or_function)
    if is_function or _is_pydantic_base_model(class_or_function):
        doc = class_or_function.__doc__
    else:
        doc = class_or_function.__init__.__doc__ or class_or_function.__doc__
    return parse(doc)


def _tap_data(class_or_function: _ClassOrFunction, param_to_description: dict[str, str], func_kwargs) -> _TapData:
    """
    Controls how :class:`_TapData` is extracted from `class_or_function`.
    """
    is_pydantic_v1_data_model = _IS_PYDANTIC_V1 and (
        _is_pydantic_base_model(class_or_function) or _is_pydantic_dataclass(class_or_function)
    )
    if _is_data_model(class_or_function) and not is_pydantic_v1_data_model:
        # Data models from Pydantic v1 don't lend themselves well to _tap_data_from_data_model.
        # _tap_data_from_data_model looks at the data model's fields. In Pydantic v1, the field.type_ attribute stores
        # the field's annotation/type. But (in Pydantic v1) there's a bug where field.type_ is set to the inner-most
        # type of a subscripted type. For example, annotating a field with list[str] causes field.type_ to be str, not
        # list[str]. To get around this, we'll extract _TapData by looking at the signature of the data model
        return _tap_data_from_data_model(class_or_function, func_kwargs, param_to_description)
        # TODO: allow passing func_kwargs to a Pydantic BaseModel
    return _tap_data_from_class_or_function(class_or_function, func_kwargs, param_to_description)


def _tap_class(args_data: Sequence[_ArgData]) -> type[Tap]:
    """
    Transfers argument data to a :class:`tap.Tap` class. Arguments will be added to the parser on initialization.
    """

    class ArgParser(Tap):
        # Overwriting configure would force a user to remember to call super().configure if they want to overwrite it
        # Instead, overwrite _configure
        def _configure(self):
            for arg_data in args_data:
                variable = arg_data.name
                self._annotations[variable] = str if arg_data.annotation is Any else arg_data.annotation
                self.class_variables[variable] = {"comment": arg_data.description or ""}
                if arg_data.is_required:
                    kwargs = {}
                else:
                    kwargs = dict(required=False, default=arg_data.default)
                self.add_argument(f"--{variable}", **kwargs)

            super()._configure()

    return ArgParser


def to_tap_class(class_or_function: _ClassOrFunction) -> type[Tap]:
    """Creates a `Tap` class from `class_or_function`. This can be subclassed to add custom argument handling and
    instantiated to create a typed argument parser.

    :param class_or_function: The class or function to run with the provided arguments.
    """
    docstring = _docstring(class_or_function)
    param_to_description = {param.arg_name: param.description for param in docstring.params}
    # TODO: add func_kwargs
    tap_data = _tap_data(class_or_function, param_to_description, func_kwargs={})
    return _tap_class(tap_data.args_data)


def tapify(
    class_or_function: Union[Callable[..., OutputType], type[OutputType]],
    known_only: bool = False,
    command_line_args: Optional[list[str]] = None,
    explicit_bool: bool = False,
    underscores_to_dashes: bool = False,
    description: Optional[str] = None,
    **func_kwargs,
) -> OutputType:
    """Tapify initializes a class or runs a function by parsing arguments from the command line.

    :param class_or_function: The class or function to run with the provided arguments.
    :param known_only: If true, ignores extra arguments and only parses known arguments.
    :param command_line_args: A list of command line style arguments to parse (e.g., `['--arg', 'value']`). If None,
                              arguments are parsed from the command line (default behavior).
    :param explicit_bool: Booleans can be specified on the command line as `--arg True` or `--arg False` rather than
                          `--arg`. Additionally, booleans can be specified by prefixes of True and False with any
                          capitalization as well as 1 or 0.
    :param underscores_to_dashes: If True, convert underscores in flag names to dashes.
    :param description: The description displayed in the help message—the same description passed in
                        `argparse.ArgumentParser(description=...)`. By default, it's extracted from `class_or_function`'s
                        docstring.
    :param func_kwargs: Additional keyword arguments for the function. These act as default values when parsing the
                        command line arguments and overwrite the function defaults but are overwritten by the parsed
                        command line arguments.
    """
    # We don't directly call to_tap_class b/c we need tap_data, not just tap_class
    docstring = _docstring(class_or_function)
    param_to_description = {param.arg_name: param.description for param in docstring.params}
    tap_data = _tap_data(class_or_function, param_to_description, func_kwargs)
    tap_class = _tap_class(tap_data.args_data)
    # Create a Tap object
    if description is None:
        description = "\n".join(filter(None, (docstring.short_description, docstring.long_description)))
    tap = tap_class(description=description, explicit_bool=explicit_bool, underscores_to_dashes=underscores_to_dashes)

    # If any func_kwargs remain, they are not used in the function, so raise an error
    known_only = known_only or tap_data.known_only
    if func_kwargs and not known_only:
        raise ValueError(f"Unknown keyword arguments: {func_kwargs}")

    # Parse command line arguments
    command_line_args: Tap = tap.parse_args(args=command_line_args, known_only=known_only)

    # Prepare command line arguments for class_or_function, respecting positional-only args
    class_or_function_args: list[Any] = []
    class_or_function_kwargs: dict[str, Any] = {}
    command_line_args_dict = command_line_args.as_dict()
    for arg_data in tap_data.args_data:
        arg_value = command_line_args_dict[arg_data.name]
        if arg_data.is_positional_only:
            class_or_function_args.append(arg_value)
        else:
            class_or_function_kwargs[arg_data.name] = arg_value

    # Get **kwargs from extra command line arguments
    if tap_data.has_kwargs:
        kwargs = {tap.extra_args[i].lstrip("-"): tap.extra_args[i + 1] for i in range(0, len(tap.extra_args), 2)}
        class_or_function_kwargs.update(kwargs)

    # Initialize the class or run the function with the parsed arguments
    return class_or_function(*class_or_function_args, **class_or_function_kwargs)
