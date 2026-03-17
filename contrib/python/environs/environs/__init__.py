from __future__ import annotations

import collections
import contextlib
import functools
import inspect
import json as pyjson
import os
import re
import typing
from collections.abc import Mapping
from pathlib import Path

import marshmallow as ma
from dotenv.main import _walk_to_root, load_dotenv

from . import fields
from .exceptions import (
    EnvError,
    EnvSealedError,
    EnvValidationError,
    ParserConflictError,
)

if typing.TYPE_CHECKING:
    import datetime as dt
    import decimal
    import uuid
    from urllib.parse import ParseResult

    from .types import (
        DictFieldMethod,
        EnumFieldMethod,
        ErrorMapping,
        FieldFactory,
        FieldMethod,
        ListFieldMethod,
        ParserMethod,
        Subcast,
        TimeDeltaFieldMethod,
    )

    try:
        from dj_database_url import DBConfig
    except ImportError:
        pass

__all__ = ["Env", "EnvError", "FileAwareEnv", "ValidationError", "env"]

_T = typing.TypeVar("_T")
_StrType = str
_BoolType = bool
_IntType = int
_ListType = list
_DictType = dict

_EXPANDED_VAR_PATTERN = re.compile(r"(?<!\\)\$\{([A-Za-z0-9_]+)(:-[^\}:]*)?\}")


# Reexport marshmallow's validate module and ValidationError.
validate = ma.validate
# Custom validators should raise this for invalid input.
ValidationError = ma.ValidationError


def _field2method(
    field_or_factory: type[ma.fields.Field] | FieldFactory,
    method_name: str,
    *,
    preprocess: typing.Callable | None = None,
    preprocess_kwarg_names: typing.Sequence[str] = (),
) -> typing.Any:
    def method(
        self: Env,
        name: str,
        default: typing.Any = ...,
        subcast: Subcast[_T] | None = None,
        *,
        # Subset of relevant marshmallow.Field kwargs
        validate: (
            typing.Callable[[typing.Any], typing.Any]
            | typing.Iterable[typing.Callable[[typing.Any], typing.Any]]
            | None
        ) = None,
        # Additional kwargs are passed to Field constructor
        **kwargs,
    ) -> _T | None:
        if self._sealed:
            raise EnvSealedError(
                "Env has already been sealed. New values cannot be parsed.",
            )
        load_default = default if default is not Ellipsis else ma.missing
        preprocess_kwargs = {
            name: kwargs.pop(name) for name in preprocess_kwarg_names if name in kwargs
        }
        if isinstance(field_or_factory, type) and issubclass(
            field_or_factory,
            ma.fields.Field,
        ):
            field = field_or_factory(
                validate=validate,
                load_default=load_default,
                **kwargs,
            )
        else:
            parsed_subcast = _make_subcast_field(subcast) if subcast else ma.fields.Raw
            field = typing.cast("FieldFactory", field_or_factory)(
                subcast=parsed_subcast,
                validate=validate,
                load_default=load_default,
            )
        parsed_key, value, proxied_key = self._get_from_environ(name, default=Ellipsis)
        self._fields[parsed_key] = field
        source_key = proxied_key or parsed_key
        if value is Ellipsis:
            if default is not Ellipsis:
                self._values[parsed_key] = default
                return default
            if self.eager:
                raise EnvError(
                    f'Environment variable "{proxied_key or parsed_key}" not set',
                )
            self._errors[parsed_key].append("Environment variable not set.")
            return None
        try:
            if preprocess:
                value = preprocess(value, **preprocess_kwargs)
            value = field.deserialize(value)
        except ma.ValidationError as error:
            if self.eager:
                raise EnvValidationError(
                    f'Environment variable "{source_key}" invalid: {error.args[0]}',
                    error.messages,
                ) from error
            self._errors[parsed_key].extend(error.messages)
        else:
            self._values[parsed_key] = value
        return typing.cast("_T | None", value)

    method.__name__ = method_name
    return method


def _func2method(func: typing.Callable[..., _T], method_name: str) -> typing.Any:
    def method(
        self: Env,
        name: str,
        default: typing.Any = ...,
        **kwargs,
    ) -> _T | None:
        if self._sealed:
            raise EnvSealedError(
                "Env has already been sealed. New values cannot be parsed.",
            )
        parsed_key, raw_value, proxied_key = self._get_from_environ(name, default)
        self._fields[parsed_key] = ma.fields.Raw()
        source_key = proxied_key or parsed_key
        if raw_value is Ellipsis:
            if self.eager:
                raise EnvError(
                    f'Environment variable "{proxied_key or parsed_key}" not set',
                )
            self._errors[parsed_key].append("Environment variable not set.")
            return None
        if raw_value or raw_value == "":  # noqa: SIM108
            value = raw_value
        else:
            value = None
        try:
            value = func(raw_value, **kwargs)
        except (EnvError, ma.ValidationError) as error:
            messages = (
                error.messages
                if isinstance(error, ma.ValidationError)
                else [error.args[0]]
            )
            if self.eager:
                raise EnvValidationError(
                    f'Environment variable "{source_key}" invalid: {error.args[0]}',
                    messages,
                ) from error
            self._errors[parsed_key].extend(messages)
        else:
            self._values[parsed_key] = value
        return typing.cast("_T | None", value)

    method.__name__ = method_name
    return method


def _make_subcast_field(
    subcast: Subcast,
) -> type[ma.fields.Field]:
    if isinstance(subcast, type) and subcast in ma.Schema.TYPE_MAPPING:
        inner_field = ma.Schema.TYPE_MAPPING[subcast]
    elif isinstance(subcast, type) and issubclass(subcast, ma.fields.Field):
        inner_field = subcast
    elif callable(subcast):

        class SubcastField(ma.fields.Field):
            def _deserialize(self, value, *args, **kwargs):
                return subcast(value)

        inner_field = SubcastField
    else:
        inner_field = ma.fields.Raw
    return inner_field


def _make_list_field(*, subcast: Subcast | None, **kwargs) -> ma.fields.List:
    inner_field = _make_subcast_field(subcast) if subcast else ma.fields.Raw
    return ma.fields.List(inner_field, **kwargs)


def _preprocess_list(
    value: str | typing.Iterable,
    *,
    delimiter: str = ",",
    **kwargs,
) -> typing.Iterable:
    if ma.utils.is_iterable_but_not_string(value) or value is None:
        return value
    return typing.cast("str", value).split(delimiter) if value != "" else []


def _preprocess_dict(
    value: str | typing.Mapping,
    *,
    subcast_keys: Subcast | None = None,
    subcast_values: Subcast | None = None,
    delimiter: str = ",",
    key_value_delimiter: str = "=",
    **kwargs,
) -> typing.Mapping:
    if isinstance(value, Mapping):
        return value
    subcast_keys_instance: ma.fields.Field
    if subcast_keys:
        subcast_keys_instance = _make_subcast_field(subcast_keys)(**kwargs)
    else:
        subcast_keys_instance = ma.fields.Raw()
    subcast_values_instance: ma.fields.Field
    if subcast_values:
        subcast_values_instance = _make_subcast_field(subcast_values)(**kwargs)
    else:
        subcast_values_instance = ma.fields.Raw()

    return {
        subcast_keys_instance.deserialize(
            key.strip(),
        ): subcast_values_instance.deserialize(val.strip())
        for key, val in (
            item.split(key_value_delimiter, 1) for item in value.split(delimiter) if value
        )
    }


def _preprocess_json(value: str | typing.Mapping | list, **kwargs):
    try:
        if isinstance(value, str):
            return pyjson.loads(value)
        if isinstance(value, (dict, list)) or value is None:
            return value
        raise ma.ValidationError("Not valid JSON.")
    except pyjson.JSONDecodeError as error:
        raise ma.ValidationError("Not valid JSON.") from error


def _dj_db_url_parser(value: str, **kwargs) -> DBConfig:
    try:
        import dj_database_url  # noqa: PLC0415
    except ImportError as error:
        raise RuntimeError(
            "The dj_db_url parser requires the dj-database-url package. "
            "You can install it with: pip install dj-database-url",
        ) from error
    try:
        return dj_database_url.parse(value, **kwargs)
    except Exception as error:
        raise ma.ValidationError("Not a valid database URL.") from error


def _dj_email_url_parser(value: str, **kwargs) -> dict:
    try:
        import dj_email_url  # noqa: PLC0415
    except ImportError as error:
        raise RuntimeError(
            "The dj_email_url parser requires the dj-email-url package. "
            "You can install it with: pip install dj-email-url",
        ) from error
    try:
        return dj_email_url.parse(value, **kwargs)
    except Exception as error:
        raise ma.ValidationError("Not a valid email URL.") from error


def _dj_cache_url_parser(value: str, **kwargs) -> dict:
    try:
        import django_cache_url  # noqa: PLC0415
    except ImportError as error:
        raise RuntimeError(
            "The dj_cache_url parser requires the django-cache-url package. "
            "You can install it with: pip install django-cache-url",
        ) from error
    try:
        return django_cache_url.parse(value, **kwargs)
    except Exception as error:
        # django_cache_url may raise Exception("Unknown backend...")
        #   so use that error message in the validation error
        raise ma.ValidationError(error.args[0]) from error


class Env:
    """An environment variable reader."""

    __call__: FieldMethod[typing.Any] = _field2method(ma.fields.Raw, "__call__")

    int: FieldMethod[int] = _field2method(ma.fields.Int, "int")
    bool: FieldMethod[bool] = _field2method(ma.fields.Bool, "bool")
    str: FieldMethod[str] = _field2method(ma.fields.Str, "str")
    float: FieldMethod[float] = _field2method(ma.fields.Float, "float")
    decimal: FieldMethod[decimal.Decimal] = _field2method(ma.fields.Decimal, "decimal")
    list: ListFieldMethod = _field2method(
        _make_list_field,
        "list",
        preprocess=_preprocess_list,
        preprocess_kwarg_names=("subcast", "delimiter"),
    )
    dict: DictFieldMethod = _field2method(
        ma.fields.Dict,
        "dict",
        preprocess=_preprocess_dict,
        preprocess_kwarg_names=(
            "subcast_keys",
            "subcast_key",
            "subcast_values",
            "delimiter",
            "key_value_delimiter",
        ),
    )
    json: FieldMethod[_ListType | _DictType] = _field2method(
        ma.fields.Raw,
        "json",
        preprocess=_preprocess_json,
    )
    datetime: FieldMethod[dt.datetime] = _field2method(ma.fields.DateTime, "datetime")
    date: FieldMethod[dt.date] = _field2method(ma.fields.Date, "date")
    time: FieldMethod[dt.time] = _field2method(ma.fields.Time, "time")
    timedelta: TimeDeltaFieldMethod = _field2method(fields.TimeDelta, "timedelta")
    path: FieldMethod[Path] = _field2method(fields.Path, "path")
    log_level: FieldMethod[_IntType] = _field2method(fields.LogLevel, "log_level")

    uuid: FieldMethod[uuid.UUID] = _field2method(ma.fields.UUID, "uuid")
    url: FieldMethod[ParseResult] = _field2method(fields.Url, "url")

    enum: EnumFieldMethod = _field2method(ma.fields.Enum, "enum")
    #dj_db_url = _func2method(_dj_db_url_parser, "dj_db_url")
    #dj_email_url = _func2method(_dj_email_url_parser, "dj_email_url")
    #dj_cache_url = _func2method(_dj_cache_url_parser, "dj_cache_url")

    def __init__(
        self,
        *,
        eager: _BoolType = True,
        expand_vars: _BoolType = False,
        prefix: _StrType | None = None,
    ):
        self.eager = eager
        self._sealed: bool = False
        self.expand_vars = expand_vars
        self._fields: dict[_StrType, ma.fields.Field] = {}
        self._values: dict[_StrType, typing.Any] = {}
        self._errors: ErrorMapping = collections.defaultdict(list)
        self._prefix: _StrType | None = prefix
        self.__custom_parsers__: dict[_StrType, ParserMethod] = {}

    def __repr__(self) -> _StrType:
        return f"<{self.__class__.__name__}(eager={self.eager}, expand_vars={self.expand_vars})>"

    @staticmethod
    def read_env(
        path: _StrType | Path | None = None,
        *,
        recurse: _BoolType = True,
        verbose: _BoolType = False,
        override: _BoolType = False,
        return_path: _BoolType = False,
    ) -> _BoolType | _StrType | None:
        """Read a .env file into os.environ.

        If .env is not found in the directory from which this method is called,
        the default behavior is to recurse up the directory tree until a .env
        file is found. If you do not wish to recurse up the tree, you may pass
        False as a second positional argument.
        """
        env_path = None
        is_env_loaded = False
        if path is None:
            # By default, start search from the same directory this function is called
            caller_dir = Path(os.path.realpath('.')).resolve()  # Start from current directory in arcadia build
            start = caller_dir / ".env"
        else:
            if Path(path).is_dir():
                raise ValueError("path must be a filename, not a directory.")
            start = Path(path)
        if recurse:
            start_dir, env_name = os.path.split(start)
            if not start_dir:  # Only a filename was given
                start_dir = os.getcwd()
            for dirname in _walk_to_root(start_dir):
                check_path = Path(dirname) / env_name
                if check_path.exists():
                    is_env_loaded = load_dotenv(
                        check_path,
                        verbose=verbose,
                        override=override,
                    )
                    env_path = str(check_path)
                    break

        else:
            is_env_loaded = load_dotenv(str(start), verbose=verbose, override=override)
            env_path = str(start)

        if return_path:
            return env_path
        return is_env_loaded

    @contextlib.contextmanager
    def prefixed(self, prefix: _StrType) -> typing.Iterator[Env]:
        """Context manager for parsing envvars with a common prefix."""
        try:
            old_prefix = self._prefix
            if old_prefix is None:
                self._prefix = prefix
            else:
                self._prefix = f"{old_prefix}{prefix}"
            yield self
        finally:
            # explicitly reset the stored prefix on completion and exceptions
            self._prefix = None
        self._prefix = old_prefix

    def seal(self):
        """Validate parsed values and prevent new values from being added.

        :raises: environs.EnvValidationError
        """
        self._sealed = True
        if self._errors:
            error_messages = dict(self._errors)
            self._errors = {}
            raise EnvValidationError(
                f"Environment variables invalid: {error_messages}",
                error_messages,
            )

    def __getattr__(self, name: _StrType):
        try:
            return functools.partial(self.__custom_parsers__[name], self)
        except KeyError as error:
            raise AttributeError(f"{self} has no attribute {name}") from error

    def add_parser(self, name: _StrType, func: typing.Callable) -> None:
        """Register a new parser method with the name ``name``. ``func`` must
        receive the input value for an environment variable.
        """
        if hasattr(self, name):
            raise ParserConflictError(
                f"Env already has a method with name '{name}'. Use a different name.",
            )
        self.__custom_parsers__[name] = _func2method(func, method_name=name)

    def parser_for(
        self,
        name: _StrType,
    ) -> typing.Callable[[typing.Callable], typing.Callable]:
        """Decorator that registers a new parser method with the name ``name``.
        The decorated function must receive the input value for an environment variable.
        """

        def decorator(func: typing.Callable) -> typing.Callable:
            self.add_parser(name, func)
            return func

        return decorator

    def add_parser_from_field(self, name: _StrType, field_cls: type[ma.fields.Field]):
        """Register a new parser method with name ``name``,
        given a marshmallow ``Field``.
        """
        self.__custom_parsers__[name] = _field2method(field_cls, method_name=name)

    def dump(self) -> typing.Mapping[_StrType, typing.Any]:
        """Dump parsed environment variables to a dictionary of simple data types
        (numbers and strings).
        """
        schema = ma.Schema.from_dict(self._fields)()
        return schema.dump(self._values)

    def _get_from_environ(
        self,
        key: _StrType,
        default: typing.Any,
        *,
        proxied: _BoolType = False,
    ) -> tuple[_StrType, typing.Any, _StrType | None]:
        """Access a value from os.environ. Handles proxied variables,
        e.g. SMTP_LOGIN={{MAILGUN_LOGIN}}.

        Returns a tuple (envvar_key, envvar_value, proxied_key). The ``envvar_key``
        will be different from the passed key for proxied variables. proxied_key
        will be None if the envvar isn't proxied.

        The ``proxied`` flag is recursively passed if a proxy lookup is required
        to get a proxy env key.
        """
        env_key = self._get_key(key, omit_prefix=proxied)
        value = self._get_value(env_key, default)
        if hasattr(value, "strip"):
            expand_match = self.expand_vars and _EXPANDED_VAR_PATTERN.fullmatch(value)
            if expand_match:  # Full match expand_vars - special case keep default
                proxied_key: _StrType = expand_match.groups()[0]
                subs_default: _StrType | None = expand_match.groups()[1]
                if subs_default is not None:
                    default = subs_default[2:]
                elif (
                    value == default
                ):  # if we have used default, don't use it recursively
                    default = Ellipsis
                return (
                    key,
                    self._get_from_environ(proxied_key, default, proxied=True)[1],
                    proxied_key,
                )
            expand_search = self.expand_vars and _EXPANDED_VAR_PATTERN.search(value)
            if (
                expand_search
            ):  # Multiple or in text match expand_vars - General case - default lost
                return self._expand_vars(env_key, value)
            # Remove escaped $
            if self.expand_vars and r"\$" in value:
                value = value.replace(r"\$", "$")
        return env_key, value, None

    def _expand_vars(self, parsed_key, value):
        ret = ""
        prev_start = 0
        for match in _EXPANDED_VAR_PATTERN.finditer(value):
            env_key = match.group(1)
            env_default = match.group(2)
            if env_default is None:  # noqa: SIM108
                env_default = Ellipsis
            else:
                env_default = env_default[2:]  # trim ':-' from default
            _, env_value, _ = self._get_from_environ(env_key, env_default, proxied=True)
            if env_value is Ellipsis:
                return parsed_key, env_value, env_key
            ret += value[prev_start : match.start()] + env_value
            prev_start = match.end()
        ret += value[prev_start:]

        return parsed_key, ret, env_key

    def _get_key(self, key: _StrType, *, omit_prefix: _BoolType = False) -> _StrType:
        return self._prefix + key if self._prefix and not omit_prefix else key

    def _get_value(self, env_key: _StrType, default: typing.Any) -> typing.Any:
        return os.environ.get(env_key, default)


class FileAwareEnv(Env):
    """An environment variable reader that supports reading values from files."""

    def __init__(
        self,
        *,
        file_suffix: _StrType = "_FILE",
        eager: _BoolType = True,
        expand_vars: _BoolType = False,
        prefix: _StrType | None = None,
        strip_whitespace: _BoolType = False,
    ):
        self.file_suffix: _StrType = file_suffix
        self.strip_whitespace: _BoolType = strip_whitespace
        super().__init__(eager=eager, expand_vars=expand_vars, prefix=prefix)

    def _get_value(self, env_key: _StrType, default: typing.Any) -> typing.Any:
        """Return the contents of the file referenced in key <env_key>_FILE, if present."""
        file_key = f"{env_key}{self.file_suffix}"
        if file_path := os.environ.get(file_key, None):
            try:
                value: _StrType = Path(file_path).read_text()
                return value.strip() if self.strip_whitespace else value
            except (FileNotFoundError, IsADirectoryError, PermissionError) as err:
                raise ValueError(
                    f"The value of {file_key} must be a readable file path."
                ) from err
        return super()._get_value(env_key, default)


# Singleton instance, for convenience
env = Env()
