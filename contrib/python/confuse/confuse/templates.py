from __future__ import annotations

import enum
import os
import pathlib
import re
from collections import abc
from collections.abc import Hashable, Iterable, Mapping
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any, Generic, NoReturn, overload

from typing_extensions import TypeVar

from . import exceptions, util

if TYPE_CHECKING:
    from .core import ConfigView, Subview


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
K = TypeVar("K", bound=Hashable, default=str)
P = TypeVar("P", bound=pathlib.PurePath | str, default=str)
V = TypeVar("V", default=object)
ConfigKey = int | str | bytes
ConfigKeyT = TypeVar("ConfigKeyT", bound=ConfigKey, default=str)


class _Required:
    """Marker class for required sentinel."""

    pass


REQUIRED = _Required()
"""A sentinel indicating that there is no default value and an exception
should be raised when the value is missing.
"""


class AttrDict(dict[ConfigKeyT, V]):
    """A `dict` subclass that can be accessed via attributes (dot
    notation) for convenience.
    """

    def __getattr__(self, key: str) -> V:
        if key in self:
            return self[key]  # type: ignore[index]
        else:
            raise AttributeError(key)

    def __setattr__(self, key: str, value: V) -> None:
        self[key] = value  # type: ignore[index]


class Template(Generic[T_co]):
    """A value template for configuration fields.

    The template works like a type and instructs Confuse about how to
    interpret a deserialized YAML value. This includes type conversions,
    providing a default value, and validating for errors. For example, a
    filepath type might expand tildes and check that the file exists.
    """

    default: T_co | _Required

    @overload
    def __init__(self: Template[T], default: T) -> None: ...
    @overload
    def __init__(self: Template[Any], default: _Required = ...) -> None: ...
    def __init__(self, default: object = REQUIRED) -> None:
        """Create a template with a given default value.

        If `default` is the sentinel `REQUIRED` (as it is by default),
        then an error will be raised when a value is missing. Otherwise,
        missing values will instead return `default`.
        """
        self.default = default  # type: ignore[assignment]

    def __call__(self, view: ConfigView) -> T_co:
        """Invoking a template on a view gets the view's value according
        to the template.
        """
        return self.value(view, self)

    def value(
        self, view: ConfigView, template: Template[T_co] | object | None = None
    ) -> T_co:
        """Get the value for a `ConfigView`.

        May raise a `NotFoundError` if the value is missing (and the
        template requires it) or a `ConfigValueError` for invalid values.
        """
        try:
            value, _ = view.first()
            return self.convert(value, view)
        except exceptions.NotFoundError:
            pass

        # Get default value, or raise if required.
        return self.get_default_value(view.name)

    def get_default_value(self, key_name: str = "default") -> T_co:
        """Get the default value to return when the value is missing.

        May raise a `NotFoundError` if the value is required.
        """
        if not hasattr(self, "default") or self.default is REQUIRED:
            # The value is required. A missing value is an error.
            raise exceptions.NotFoundError(f"{key_name} not found")
        # The value is not required.
        return self.default  # type: ignore[return-value]

    def convert(self, value: Any, view: ConfigView) -> T_co:
        """Convert the YAML-deserialized value to a value of the desired
        type.

        Subclasses should override this to provide useful conversions.
        May raise a `ConfigValueError` when the configuration is wrong.
        """
        # Default implementation does no conversion.
        return value  # type: ignore[no-any-return]

    def fail(
        self, message: str, view: ConfigView, type_error: bool = False
    ) -> NoReturn:
        """Raise an exception indicating that a value cannot be
        accepted.

        `type_error` indicates whether the error is due to a type
        mismatch rather than a malformed value. In this case, a more
        specific exception is raised.
        """
        exc_class = (
            exceptions.ConfigTypeError if type_error else exceptions.ConfigValueError
        )
        raise exc_class(f"{view.name}: {message}")

    def __repr__(self) -> str:
        return "{}({})".format(
            type(self).__name__,
            "" if self.default is REQUIRED else repr(self.default),
        )


class Integer(Template[int]):
    """An integer configuration value template."""

    def convert(self, value: int | float, view: ConfigView) -> int:
        """Check that the value is an integer. Floats are rounded."""
        if isinstance(value, int):
            return value
        elif isinstance(value, float):
            return int(value)
        else:
            self.fail("must be a number", view, True)


Numeric = TypeVar("Numeric", int, float)


class Number(Template[Numeric]):
    """A numeric type: either an integer or a floating-point number."""

    def convert(self, value: Numeric, view: ConfigView) -> Numeric:
        """Check that the value is an int or a float."""
        if isinstance(value, (int, float)):
            return value
        else:
            self.fail(f"must be numeric, not {type(value).__name__}", view, True)


class MappingTemplate(Template[AttrDict[ConfigKeyT, V]]):
    """A template that uses a dictionary to specify other types for the
    values for a set of keys and produce a validated `AttrDict`.
    """

    def __init__(self, mapping: Mapping[ConfigKeyT, Template[V] | type[V]]) -> None:
        """Create a template according to a dict (mapping). The
        mapping's values should themselves either be Types or
        convertible to Types.
        """
        subtemplates: dict[ConfigKeyT, Template[V]] = {}
        for key, typ in mapping.items():
            subtemplates[key] = as_template(typ)
        self.subtemplates = subtemplates

    def value(
        self,
        view: ConfigView,
        template: Template[AttrDict[ConfigKeyT, V]] | object | None = None,
    ) -> AttrDict[ConfigKeyT, V]:
        """Get a dict with the same keys as the template and values
        validated according to the value types.
        """
        return AttrDict(
            {k: v.value(view[k], self) for k, v in self.subtemplates.items()}
        )

    def __repr__(self) -> str:
        return f"MappingTemplate({self.subtemplates!r})"


class Sequence(Template[list[T]]):
    """A template used to validate lists of similar items,
    based on a given subtemplate.
    """

    subtemplate: Template[T]

    @overload
    def __init__(
        self: Sequence[dict[str, V]], subtemplate: Mapping[str, Template[V] | type[V]]
    ) -> None: ...
    @overload
    def __init__(self, subtemplate: type[T]) -> None: ...
    @overload
    def __init__(self, subtemplate: Template[T]) -> None: ...

    def __init__(self, subtemplate: Template[T] | type[T] | Mapping[str, object]):
        """Create a template for a list with items validated
        on a given subtemplate.
        """
        super().__init__()
        self.subtemplate = as_template(subtemplate)  # type: ignore[assignment]

    def value(
        self, view: ConfigView, template: Template[list[T]] | object | None = None
    ) -> list[T]:
        """Get a list of items validated against the template."""
        out = []
        for item in view.sequence():
            out.append(self.subtemplate.value(item, self))
        return out

    def __repr__(self) -> str:
        return f"Sequence({self.subtemplate!r})"


class MappingValues(Template[dict[str, T]]):
    """A template used to validate mappings of similar items,
    based on a given subtemplate applied to the values.

    All keys in the mapping are considered valid, but values
    must pass validation by the subtemplate. Similar to the
    Sequence template but for mappings.
    """

    subtemplate: Template[T]

    def __init__(self, subtemplate: Template[T] | type[T] | Mapping[str, object]):
        """Create a template for a mapping with variable keys
        and item values validated on a given subtemplate.
        """
        super().__init__()
        self.subtemplate = as_template(subtemplate)  # type: ignore[assignment]

    def value(
        self, view: ConfigView, template: Template[dict[str, T]] | object | None = None
    ) -> dict[str, T]:
        """Get a dict with the same keys as the view and the
        value of each item validated against the subtemplate.
        """
        out = {}
        for key, item in view.items():
            out[key] = self.subtemplate.value(item, self)
        return out

    def __repr__(self) -> str:
        return f"MappingValues({self.subtemplate!r})"


class String(Template[str]):
    """A string configuration value template."""

    def __init__(
        self,
        default: str | _Required = REQUIRED,
        pattern: str | None = None,
        expand_vars: bool = False,
    ):
        """Create a template with the added optional `pattern` argument,
        a regular expression string that the value should match.
        """
        super().__init__(default)
        self.pattern = pattern
        self.expand_vars = expand_vars
        if pattern:
            self.regex = re.compile(pattern)

    def __repr__(self) -> str:
        args = []

        if self.default is not REQUIRED:
            args.append(repr(self.default))

        if self.pattern is not None:
            args.append("pattern=" + repr(self.pattern))

        return f"String({', '.join(args)})"

    def convert(self, value: object, view: ConfigView) -> str:
        """Check that the value is a string and matches the pattern."""
        if not isinstance(value, str):
            self.fail("must be a string", view, True)

        if self.pattern and not self.regex.match(value):
            self.fail(f"must match the pattern {self.pattern}", view)

        if self.expand_vars:
            return os.path.expandvars(value)
        else:
            return value


class Choice(Template[T], Generic[T, K]):
    """A template that permits values from a sequence of choices.

    Sequences, dictionaries and :class:`Enum` types are supported,
    see :meth:`__init__` for usage.
    """

    choices: abc.Sequence[T] | dict[K, T] | type[T]

    def __init__(
        self,
        choices: abc.Sequence[T] | dict[K, T] | type[T],
        default: T | _Required = REQUIRED,
    ) -> None:
        """Create a template that validates any of the values from the
        iterable `choices`.

        If `choices` is a map, then the corresponding value is emitted.
        Otherwise, the value itself is emitted.

        If `choices` is a `Enum`, then the enum entry with the value is
        emitted.
        """
        super().__init__(default)
        self.choices = choices

    @singledispatchmethod
    def convert_choices(
        self, choices: abc.Sequence[T] | dict[K, T] | type[T], value: str
    ) -> T:
        raise NotImplementedError

    @convert_choices.register(type)
    def _(self, choices: type[T], value: str) -> T:
        return choices(value)  # type: ignore[call-arg]

    @convert_choices.register(dict)
    def _(self, choices: dict[K, T], value: K) -> T:
        return choices[value]

    @convert_choices.register(abc.Sequence)
    def _(self, choices: abc.Sequence[T], value: T) -> T:
        return choices[choices.index(value)]

    @singledispatchmethod
    def format_choices(self, choices: abc.Sequence[T] | enum.Enum) -> list[str]:
        raise NotImplementedError

    @format_choices.register(type)
    def _(self, choices: type[enum.Enum]) -> list[str]:
        return [c.value for c in choices]

    @format_choices.register(abc.Sequence)
    @format_choices.register(Mapping)
    def _(self, choices: Iterable[T]) -> list[str]:
        return list(map(str, choices))

    def convert(self, value: object, view: ConfigView) -> T:
        """Ensure that the value is among the choices (and remap if the
        choices are a mapping).
        """
        try:
            return self.convert_choices(self.choices, value)
        except (KeyError, ValueError):
            self.fail(
                f"must be one of {self.format_choices(self.choices)!r}, not {value!r}",
                view,
            )

    def __repr__(self) -> str:
        return f"Choice({self.choices!r})"


class OneOf(Template[T]):
    """A template that permits values complying to one of the given templates.

    When using templates that produce different types, explicitly specify
    the type parameter: ``OneOf[bool | str]([bool, String()])``
    """

    allowed: list[Template[Any]]
    template: Template[Any] | None

    def __init__(
        self,
        allowed: Iterable[Template[Any] | type[Any] | Mapping[str, object] | T],
        default: T | _Required = REQUIRED,
    ) -> None:
        super().__init__(default)
        self.allowed = [as_template(t) for t in allowed]
        self.template = None

    def __repr__(self) -> str:
        args = []

        if self.allowed is not None:
            args.append("allowed=" + repr(self.allowed))

        if self.default is not REQUIRED:
            args.append(repr(self.default))

        return f"OneOf({', '.join(args)})"

    def value(
        self, view: ConfigView, template: Template[T] | object | None = None
    ) -> T:
        self.template = template if isinstance(template, Template) else None
        return super().value(view, template)

    def convert(self, value: object, view: Subview) -> T:  # type: ignore[override]
        """Ensure that the value follows at least one template."""
        is_mapping = isinstance(self.template, MappingTemplate)

        for candidate in self.allowed:
            result: T
            try:
                if is_mapping:
                    assert self.template is not None
                    from .core import Subview

                    if isinstance(view, Subview):
                        # Use a new MappingTemplate to check the sibling value
                        next_template = MappingTemplate({view.key: candidate})
                        result = view.parent.get(next_template)[view.key]
                        return result
                    else:
                        self.fail("MappingTemplate must be used with a Subview", view)
                else:
                    result = view.get(candidate)
                    return result
            except exceptions.ConfigTemplateError:
                raise
            except exceptions.ConfigError:
                pass
            except ValueError as exc:
                raise exceptions.ConfigTemplateError(exc)

        self.fail(f"must be one of {self.allowed!r}, not {value!r}", view)


class BytesToStrMixin:
    @staticmethod
    def normalize_bytes(x: str | bytes) -> str:
        if isinstance(x, bytes):
            return x.decode("utf-8", "ignore")
        return x


class StrSeq(BytesToStrMixin, Template[list[str]]):
    """A template for values that are lists of strings.

    Validates both actual YAML string lists and single strings. Strings
    can optionally be split on whitespace.
    """

    def __init__(
        self, split: bool = True, default: list[str] | _Required = REQUIRED
    ) -> None:
        """Create a new template.

        `split` indicates whether, when the underlying value is a single
        string, it should be split on whitespace. Otherwise, the
        resulting value is a list containing a single string.
        """
        super().__init__(default)
        self.split = split

    def _convert_value(self, x: object, view: ConfigView) -> str:
        if not isinstance(x, (str, bytes)):
            self.fail("must be a list of strings", view, True)

        return self.normalize_bytes(x)

    def convert(
        self, value: str | bytes | list[str | bytes], view: ConfigView
    ) -> list[str]:
        if isinstance(value, bytes):
            value = value.decode("utf-8", "ignore")

        if isinstance(value, str):
            if self.split:
                values: Iterable[object] = value.split()
            else:
                values = [value]
        elif isinstance(value, Iterable):
            values = value
        else:
            self.fail("must be a whitespace-separated string or a list", view, True)

        return [self._convert_value(v, view) for v in values]


class Pairs(BytesToStrMixin, Template[list[tuple[str, V]]]):
    """A template for ordered key-value pairs.

    This can either be given with the same syntax as for `StrSeq` (i.e. without
    values), or as a list of strings and/or single-element mappings such as::

        - key: value
        - [key, value]
        - key

    The result is a list of two-element tuples. If no value is provided, the
    `default_value` will be returned as the second element.
    """

    default_value: V

    @overload
    def __init__(
        self: Pairs[str],
        default_value: str,
        default: list[tuple[str, str]] | _Required = REQUIRED,
    ) -> None: ...
    @overload
    def __init__(
        self: Pairs[None],
        default_value: None = None,
        default: list[tuple[str, None]] | _Required = REQUIRED,
    ) -> None: ...
    def __init__(
        self,
        default_value: str | None = None,
        default: list[tuple[str, str]] | list[tuple[str, None]] | _Required = REQUIRED,
    ) -> None:
        """Create a new template.

        `default_value` is the dictionary value returned for items that are not
        a mapping, but a single string.
        """
        super().__init__(default)  # type: ignore[arg-type]
        self.default_value = default_value  # type: ignore[assignment]

    def _convert_value(self, x: object, view: ConfigView) -> tuple[str, V]:
        if isinstance(x, (str, bytes)):
            return self.normalize_bytes(x), self.default_value

        if isinstance(x, Mapping):
            if len(x) != 1:
                self.fail("must be a single-element mapping", view, True)
            k, v = util.iter_first(x.items())
        elif isinstance(x, abc.Sequence):
            if len(x) != 2:
                self.fail("must be a two-element list", view, True)
            k, v = x
        else:
            # Is this even possible? -> Likely, if some !directive cause
            # YAML to parse this to some custom type.
            self.fail(f"must be a single string, mapping, or a list{x}", view, True)

        return self.normalize_bytes(k), self.normalize_bytes(v)  # type: ignore[return-value]

    def convert(
        self, value: list[abc.Sequence[str] | Mapping[str, str]], view: ConfigView
    ) -> list[tuple[str, V]]:
        return [self._convert_value(v, view) for v in value]


class Filename(Template[P]):
    """A template that validates strings as filenames.

    Filenames are returned as absolute, tilde-free paths.

    Relative paths are relative to the template's `cwd` argument
    when it is specified. Otherwise, if the paths come from a file,
    they will be relative to the configuration directory (see the
    `config_dir` method) by default or to the base directory of the
    config file if either the source has `base_for_paths` set to True
    or the template has `in_source_dir` set to True. Paths from sources
    without a file are relative to the current working directory. This
    helps attain the expected behavior when using command-line options.
    """

    def __init__(
        self,
        default: P | str | None | _Required = REQUIRED,
        cwd: str | None = None,
        relative_to: str | None = None,
        in_app_dir: bool = False,
        in_source_dir: bool = False,
    ) -> None:
        """`relative_to` is the name of a sibling value that is
        being validated at the same time.

        `in_app_dir` indicates whether the path should be resolved
        inside the application's config directory (even when the setting
        does not come from a file).

        `in_source_dir` indicates whether the path should be resolved
        relative to the directory containing the source file, if there is
        one, taking precedence over the application's config directory.
        """
        if default is None:
            self.default: P | _Required = default  # type: ignore[assignment]
        else:
            super().__init__(default)  # type: ignore[arg-type]
        self.cwd = cwd
        self.relative_to = relative_to
        self.in_app_dir = in_app_dir
        self.in_source_dir = in_source_dir

    def __repr__(self) -> str:
        args = []

        if self.default is not REQUIRED:
            args.append(repr(self.default))

        if self.cwd is not None:
            args.append("cwd=" + repr(self.cwd))

        if self.relative_to is not None:
            args.append("relative_to=" + repr(self.relative_to))

        if self.in_app_dir:
            args.append("in_app_dir=True")

        if self.in_source_dir:
            args.append("in_source_dir=True")

        return f"Filename({', '.join(args)})"

    def resolve_relative_to(
        self, view: Subview, template: MappingTemplate | Mapping[str, Any] | None
    ) -> str:
        if not isinstance(template, (Mapping, MappingTemplate)):
            # disallow config.get(Filename(relative_to='foo'))
            raise exceptions.ConfigTemplateError(
                "relative_to may only be used when getting multiple values."
            )

        elif self.relative_to == view.key:
            raise exceptions.ConfigTemplateError(f"{view.name} is relative to itself")

        elif self.relative_to not in view.parent.keys():
            # self.relative_to is not in the config
            self.fail(
                (f'needs sibling value "{self.relative_to}" to expand relative path'),
                view,
            )

        # Use a safe way to access subtemplates
        if isinstance(template, MappingTemplate):
            subtemplates = template.subtemplates
        else:
            # template is a Mapping
            subtemplates = {k: as_template(v) for k, v in template.items()}

        old_template = dict(subtemplates)

        # save time by skipping MappingTemplate's init loop
        next_template = MappingTemplate({})
        next_relative: str | None = self.relative_to

        # gather all the needed templates and nothing else
        while next_relative is not None:
            try:
                # pop to avoid infinite loop because of recursive
                # relative paths
                rel_to_template = old_template.pop(next_relative)
            except KeyError:
                if next_relative in subtemplates:
                    # we encountered this config key previously
                    raise exceptions.ConfigTemplateError(
                        f"{view.name} and {self.relative_to} are recursively relative"
                    )
                else:
                    raise exceptions.ConfigTemplateError(
                        f"missing template for {self.relative_to}, needed to expand "
                        f"{view.name}'s relative path"
                    )

            next_template.subtemplates[next_relative] = rel_to_template
            next_relative_val = getattr(rel_to_template, "relative_to", None)
            next_relative = (
                next_relative_val if isinstance(next_relative_val, str) else None
            )

        return view.parent.get(next_template)[self.relative_to]  # type: ignore[return-value]

    def value(
        self, view: ConfigView, template: Template[T] | object | None = None
    ) -> P:
        try:
            path, source = view.first()
        except exceptions.NotFoundError:
            return self.get_default_value(view.name)

        if not isinstance(path, (str, bytes)):
            self.fail(f"must be a filename, not {type(path).__name__}", view, True)

        if isinstance(path, bytes):
            path_str = path.decode("utf-8", "ignore")
        else:
            path_str = path

        path_str = os.path.expanduser(path_str)

        if not os.path.isabs(path_str):
            if self.cwd is not None:
                # relative to the template's argument
                path_str = os.path.join(self.cwd, path_str)

            elif self.relative_to is not None:
                path_str = os.path.join(
                    self.resolve_relative_to(view, template),
                    path_str,
                )

            elif (source.filename and self.in_source_dir) or (
                source.base_for_paths and not self.in_app_dir
            ):
                # relative to the directory the source file is in.
                path_str = os.path.join(os.path.dirname(source.filename), path_str)

            elif source.filename or self.in_app_dir:
                # From defaults: relative to the app's directory.
                path_str = os.path.join(view.root().config_dir(), path_str)

        return os.path.abspath(path_str)


class Path(Filename[pathlib.PurePath]):
    """A template that validates strings as `pathlib.Path` objects.

    Filenames are parsed equivalent to the `Filename` template and then
    converted to `pathlib.Path` objects.
    """

    def value(
        self, view: ConfigView, template: Template[pathlib.Path] | object | None = None
    ) -> pathlib.Path:
        val = super().value(view, template)
        return pathlib.Path(val) if val is not None else None


class Optional(Template[T | None]):
    """A template that makes a subtemplate optional.

    If the value is present and not null, it must validate against the
    subtemplate. However, if the value is null or missing, the template will
    still validate, returning a default value. If `allow_missing` is False,
    the template will not allow missing values while still permitting null.
    """

    subtemplate: Template[T]
    default: T | None

    @overload
    def __init__(
        self,
        subtemplate: type[T],
        default: T | None = None,
        allow_missing: bool = True,
    ) -> None: ...

    @overload
    def __init__(
        self,
        subtemplate: Template[T],
        default: T | None = None,
        allow_missing: bool = True,
    ) -> None: ...

    @overload
    def __init__(
        self,
        subtemplate: Mapping[str, object],
        default: Mapping[str, Any] | None = None,
        allow_missing: bool = True,
    ) -> None: ...

    def __init__(
        self,
        subtemplate: Template[T] | type[T] | Mapping[str, object],
        default: T | Mapping[str, Any] | None = None,
        allow_missing: bool = True,
    ) -> None:
        self.subtemplate: Template[T] = as_template(subtemplate)  # type: ignore[assignment]
        if default is None:
            # When no default is passed, try to use the subtemplate's
            # default value as the default for this template
            try:
                default = self.subtemplate.get_default_value()
            except exceptions.NotFoundError:
                pass
        self.default = default  # type: ignore[assignment]
        self.allow_missing = allow_missing

    def value(
        self, view: ConfigView, template: Template[T | None] | object | None = None
    ) -> T | None:
        try:
            value, _ = view.first()
        except exceptions.NotFoundError:
            if self.allow_missing:
                # Value is missing but not required
                return self.default
            # Value must be present even though it can be null. Raise an error.
            raise exceptions.NotFoundError(f"{view.name} not found")

        if value is None:
            # None (ie, null) is always a valid value
            return self.default
        return self.subtemplate.value(view, self)

    def __repr__(self) -> str:
        return (
            f"Optional({self.subtemplate!r}, {self.default!r}, "
            f"allow_missing={self.allow_missing})"
        )


class TypeTemplate(Template[T]):
    """A simple template that checks that a value is an instance of a
    desired Python type.
    """

    def __init__(self, typ: type[T], default: T | _Required = REQUIRED) -> None:
        """Create a template that checks that the value is an instance
        of `typ`.
        """
        super().__init__(default)
        self.typ = typ

    def convert(self, value: Any, view: ConfigView) -> T:
        if isinstance(value, self.typ):
            return value

        self.fail(
            f"must be a {self.typ.__name__}, not {type(value).__name__}",
            view,
            True,
        )


@overload
def as_template(value: Template[T]) -> Template[T]: ...
@overload
def as_template(value: type[T]) -> Template[T]: ...
@overload
def as_template(value: Mapping[str, object]) -> MappingTemplate: ...
@overload
def as_template(value: set[T]) -> Choice[T, T]: ...
@overload
def as_template(value: list[T]) -> OneOf[T]: ...
@overload
def as_template(value: pathlib.PurePath) -> Path: ...
@overload
def as_template(value: None) -> Template[None]: ...
@overload
def as_template(value: T) -> Template[T]: ...


def as_template(value: Any) -> Template[Any]:
    """Convert a simple "shorthand" Python value to a `Template`."""
    if isinstance(value, Template):
        # If it's already a Template, pass it through.
        return value
    elif isinstance(value, abc.Mapping):
        # Dictionaries work as templates.
        return MappingTemplate(value)
    elif value is int:
        return Integer()
    elif isinstance(value, int):
        return Integer(value)
    elif isinstance(value, type) and issubclass(value, str):
        return String()
    elif isinstance(value, str):
        return String(value)
    elif isinstance(value, set):
        # convert to list to avoid hash related problems
        return Choice(list(value))
    elif isinstance(value, type) and issubclass(value, enum.Enum):
        return Choice(value)
    elif isinstance(value, list):
        return OneOf(value)
    elif value is float:
        return Number()
    elif isinstance(value, float):
        return Number(value)
    elif isinstance(value, pathlib.PurePath):
        return Path(value)
    elif value is None:
        return Template(None)
    elif value is REQUIRED:
        return Template()
    elif value is dict:
        return TypeTemplate(abc.Mapping)
    elif value is list:
        return TypeTemplate(abc.Sequence)
    elif isinstance(value, type):
        return TypeTemplate(value)
    else:
        raise ValueError(f"cannot convert to template: {value!r}")
