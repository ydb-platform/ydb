"""Functions for parsing parameters."""

from __future__ import annotations

import functools
import os
import re
from datetime import datetime, time
from typing import TYPE_CHECKING, Any, Protocol

from icalendar.caselessdict import CaselessDict
from icalendar.error import JCalParsingError
from icalendar.parser.string import validate_token
from icalendar.parser_tools import (
    DEFAULT_ENCODING,
    SEQUENCE_TYPES,
)
from icalendar.timezone.tzid import tzid_from_dt

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from icalendar.enums import VALUE
    from icalendar.prop import VPROPERTY


class HasToIcal(Protocol):
    """Protocol for objects with a to_ical method."""

    def to_ical(self) -> bytes:
        """Convert to iCalendar format."""
        ...


def param_value(
    value: Sequence[str] | str | HasToIcal, always_quote: bool = False
) -> str:
    """Convert a parameter value to its iCalendar representation.

    Applies :rfc:`6868` escaping and optionally quotes the value according
    to :rfc:`5545` parameter value formatting rules.

    Parameters:
        value: The parameter value to convert. Can be a sequence, string, or
            object with a ``to_ical()`` method.
        always_quote: If ``True``, always enclose the value in double quotes.
            Defaults to ``False`` (only quote when necessary).

    Returns:
        The formatted parameter value, escaped and quoted as needed.
    """
    if isinstance(value, SEQUENCE_TYPES):
        return q_join(map(rfc_6868_escape, value), always_quote=always_quote)
    if isinstance(value, str):
        return dquote(rfc_6868_escape(value), always_quote=always_quote)
    return dquote(rfc_6868_escape(value.to_ical().decode(DEFAULT_ENCODING)))


# Could be improved


UNSAFE_CHAR = re.compile('[\x00-\x08\x0a-\x1f\x7f",:;]')
QUNSAFE_CHAR = re.compile('[\x00-\x08\x0a-\x1f\x7f"]')


def validate_param_value(value: str, quoted: bool = True) -> None:
    """Validate a parameter value for unsafe characters.

    Checks parameter values for characters that are not allowed according to
    :rfc:`5545`. Uses different validation rules for quoted and unquoted values.

    Parameters:
        value: The parameter value to validate.
        quoted: If ``True``, validate as a quoted value (allows more characters).
            If ``False``, validate as an unquoted value (stricter).
            Defaults to ``True``.

    Raises:
        ValueError: If the value contains unsafe characters for its quote state.
    """
    validator = QUNSAFE_CHAR if quoted else UNSAFE_CHAR
    if validator.findall(value):
        raise ValueError(value)


# chars presence of which in parameter value will be cause the value
# to be enclosed in double-quotes
QUOTABLE = re.compile("[,;:’]")  # noqa: RUF001


def dquote(val: str, always_quote: bool = False) -> str:
    """Enclose parameter values in double quotes when needed.

    Parameter values containing special characters ``,``, ``;``,
    ``:`` or ``'`` must be enclosed
    in double quotes according to :rfc:`5545`. Double-quote characters in the
    value are replaced with single quotes since they're forbidden in parameter
    values.

    Parameters:
        val: The parameter value to quote.
        always_quote: If ``True``, always enclose in quotes regardless of content.
            Defaults to ``False`` (only quote when necessary).

    Returns:
        The value, enclosed in double quotes if needed or requested.
    """
    # a double-quote character is forbidden to appear in a parameter value
    # so replace it with a single-quote character
    val = val.replace('"', "'")
    if QUOTABLE.search(val) or always_quote:
        return f'"{val}"'
    return val


# parsing helper
def q_split(st: str, sep: str = ",", maxsplit: int = -1) -> list[str]:
    """Split a string on a separator, respecting double quotes.

    Splits the string on the separator character, but ignores separators that
    appear inside double-quoted sections. This is needed for parsing parameter
    values that may contain quoted strings.

    Parameters:
        st: The string to split.
        sep: The separator character. Defaults to ``,``.
        maxsplit: Maximum number of splits to perform. If ``-1`` (default),
            then perform all possible splits.

    Returns:
        The split string parts.

    Examples:
        .. code-block:: pycon

            >>> from icalendar.parser import q_split
            >>> q_split('a,b,c')
            ['a', 'b', 'c']
            >>> q_split('a,"b,c",d')
            ['a', '"b,c"', 'd']
            >>> q_split('a;b;c', sep=';')
            ['a', 'b', 'c']
    """
    if maxsplit == 0:
        return [st]

    result = []
    cursor = 0
    length = len(st)
    inquote = 0
    splits = 0
    for i, ch in enumerate(st):
        if ch == '"':
            inquote = not inquote
        if not inquote and ch == sep:
            result.append(st[cursor:i])
            cursor = i + 1
            splits += 1
        if i + 1 == length or splits == maxsplit:
            result.append(st[cursor:])
            break
    return result


def q_join(lst: Sequence[str], sep: str = ",", always_quote: bool = False) -> str:
    """Join a list with a separator, quoting items as needed.

    Joins list items with the separator, applying :func:`dquote` to each item
    to add double quotes when they contain special characters.

    Parameters:
        lst: The list of items to join.
        sep: The separator to use. Defaults to ``,``.
        always_quote: If ``True``, always quote all items. Defaults to ``False``
            (only quote when necessary).

    Returns:
        The joined string with items quoted as needed.

    Examples:
        .. code-block:: pycon

            >>> from icalendar.parser import q_join
            >>> q_join(['a', 'b', 'c'])
            'a,b,c'
            >>> q_join(['plain', 'has,comma'])
            'plain,"has,comma"'
    """
    return sep.join(dquote(itm, always_quote=always_quote) for itm in lst)


def single_string_parameter(func: Callable | None = None, upper=False):
    """Create a parameter getter/setter for a single string parameter.

    Parameters:
        upper: Convert the value to uppercase
        func: The function to decorate.

    Returns:
        The property for the parameter or a decorator for the parameter
        if func is ``None``.
    """

    def decorator(func):
        name = func.__name__

        @functools.wraps(func)
        def fget(self: Parameters):
            """Get the value."""
            value = self.get(name)
            if value is not None and upper:
                value = value.upper()
            return value

        def fset(self: Parameters, value: str | None):
            """Set the value"""
            if value is None:
                fdel(self)
            else:
                if upper:
                    value = value.upper()
                self[name] = value

        def fdel(self: Parameters):
            """Delete the value."""
            self.pop(name, None)

        return property(fget, fset, fdel, doc=func.__doc__)

    if func is None:
        return decorator
    return decorator(func)


class Parameters(CaselessDict):
    """Parser and generator of Property parameter strings.

    It knows nothing of datatypes.
    Its main concern is textual structure.

    Examples:

        Modify parameters:

        .. code-block:: pycon

            >>> from icalendar import Parameters
            >>> params = Parameters()
            >>> params['VALUE'] = 'TEXT'
            >>> params.value
            'TEXT'
            >>> params
            Parameters({'VALUE': 'TEXT'})

        Create new parameters:

        .. code-block:: pycon

            >>> params = Parameters(value="BINARY")
            >>> params.value
            'BINARY'

        Set a default:

        .. code-block:: pycon

            >>> params = Parameters(value="BINARY", default_value="TEXT")
            >>> params
            Parameters({'VALUE': 'BINARY'})

    """

    def __init__(self, *args, **kwargs):
        """Create new parameters."""
        if args and args[0] is None:
            # allow passing None
            args = args[1:]
        defaults = {
            key[8:]: kwargs.pop(key)
            for key in list(kwargs.keys())
            if key.lower().startswith("default_")
        }
        super().__init__(*args, **kwargs)
        for key, value in defaults.items():
            self.setdefault(key, value)

    # The following paremeters must always be enclosed in double quotes
    always_quoted = (
        "ALTREP",
        "DELEGATED-FROM",
        "DELEGATED-TO",
        "DIR",
        "MEMBER",
        "SENT-BY",
        # Part of X-APPLE-STRUCTURED-LOCATION
        "X-ADDRESS",
        "X-TITLE",
        # RFC 9253
        "LINKREL",
    )
    # this is quoted should one of the values be present
    quote_also = {
        # This is escaped in the RFC
        "CN": " '",
    }

    def params(self):
        """In RFC 5545 keys are called parameters, so this is to be consitent
        with the naming conventions.
        """
        return self.keys()

    def to_ical(self, sorted: bool = True):  # noqa: A002
        """Returns an :rfc:`5545` representation of the parameters.

        Parameters:
            sorted (bool): Sort the parameters before encoding.
            exclude_utc (bool): Exclude TZID if it is set to ``"UTC"``
        """
        result = []
        items = list(self.items())
        if sorted:
            items.sort()

        for key, value in items:
            if key == "TZID" and value == "UTC":
                # The "TZID" property parameter MUST NOT be applied to DATE-TIME
                # properties whose time values are specified in UTC.
                continue
            upper_key = key.upper()
            check_quoteable_characters = self.quote_also.get(key.upper())
            always_quote = upper_key in self.always_quoted or (
                check_quoteable_characters
                and any(c in value for c in check_quoteable_characters)
            )
            quoted_value = param_value(value, always_quote=always_quote)
            if isinstance(quoted_value, str):
                quoted_value = quoted_value.encode(DEFAULT_ENCODING)
            # CaselessDict keys are always unicode
            result.append(upper_key.encode(DEFAULT_ENCODING) + b"=" + quoted_value)
        return b";".join(result)

    @classmethod
    def from_ical(cls, st, strict=False):
        """Parses the parameter format from ical text format."""

        # parse into strings
        result = cls()
        for param in q_split(st, ";"):
            try:
                key, val = q_split(param, "=", maxsplit=1)
                validate_token(key)
                # Property parameter values that are not in quoted
                # strings are case insensitive.
                vals = []
                for v in q_split(val, ","):
                    if v.startswith('"') and v.endswith('"'):
                        v2 = v.strip('"')
                        validate_param_value(v2, quoted=True)
                        vals.append(rfc_6868_unescape(v2))
                    else:
                        validate_param_value(v, quoted=False)
                        if strict:
                            vals.append(rfc_6868_unescape(v.upper()))
                        else:
                            vals.append(rfc_6868_unescape(v))
                if not vals:
                    result[key] = val
                elif len(vals) == 1:
                    result[key] = vals[0]
                else:
                    result[key] = vals
            except ValueError as exc:  # noqa: PERF203
                raise ValueError(
                    f"{param!r} is not a valid parameter string: {exc}"
                ) from exc
        return result

    @single_string_parameter(upper=True)
    def value(self) -> VALUE | str | None:
        """The VALUE parameter from :rfc:`5545`.

        Description:
            This parameter specifies the value type and format of
            the property value.  The property values MUST be of a single value
            type.  For example, a "RDATE" property cannot have a combination
            of DATE-TIME and TIME value types.

            If the property's value is the default value type, then this
            parameter need not be specified.  However, if the property's
            default value type is overridden by some other allowable value
            type, then this parameter MUST be specified.

            Applications MUST preserve the value data for x-name and iana-
            token values that they don't recognize without attempting to
            interpret or parse the value data.

        For convenience, using this property, the value will be converted to
        an uppercase string.

        .. code-block:: pycon

            >>> from icalendar import Parameters
            >>> params = Parameters()
            >>> params.value = "unknown"
            >>> params
            Parameters({'VALUE': 'UNKNOWN'})

        """

    def _parameter_value_to_jcal(
        self, value: str | float | list | VPROPERTY
    ) -> str | int | float | list[str] | list[int] | list[float]:
        """Convert a parameter value to jCal format.

        Parameters:
            value: The parameter value

        Returns:
            The jCal representation of the parameter value
        """
        if isinstance(value, list):
            return [self._parameter_value_to_jcal(v) for v in value]
        if hasattr(value, "to_jcal"):
            # proprty values respond to this
            jcal = value.to_jcal()
            # we only need the value part
            if len(jcal) == 4:
                return jcal[3]
            return jcal[3:]
        for t in (int, float, str):
            if isinstance(value, t):
                return t(value)
        raise TypeError(
            "Unsupported parameter value type for jCal conversion: "
            f"{type(value)} {value!r}"
        )

    def to_jcal(self, exclude_utc=False) -> dict[str, str]:
        """Return the jCal representation of the parameters.

        Parameters:
            exclude_utc (bool): Exclude the TZID parameter if it is UTC
        """
        jcal = {
            k.lower(): self._parameter_value_to_jcal(v)
            for k, v in self.items()
            if k.lower() != "value"
        }
        if exclude_utc and jcal.get("tzid") == "UTC":
            del jcal["tzid"]
        return jcal

    @single_string_parameter
    def tzid(self) -> str | None:
        """The TZID parameter from :rfc:`5545`."""

    def is_utc(self):
        """Whether the TZID parameter is UTC."""
        return self.tzid == "UTC"

    def update_tzid_from(self, dt: datetime | time | Any) -> None:
        """Update the TZID parameter from a datetime object.

        This sets the TZID parameter or deletes it according to the datetime.
        """
        if isinstance(dt, (datetime, time)):
            self.tzid = tzid_from_dt(dt)

    @classmethod
    def from_jcal(cls, jcal: dict[str : str | list[str]]):
        """Parse jCal parameters."""
        if not isinstance(jcal, dict):
            raise JCalParsingError("The parameters must be a mapping.", cls)
        for name, value in jcal.items():
            if not isinstance(name, str):
                raise JCalParsingError(
                    "All parameter names must be strings.", cls, value=name
                )
            if not (
                (
                    isinstance(value, list)
                    and all(isinstance(v, (str, int, float)) for v in value)
                    and value
                )
                or isinstance(value, (str, int, float))
            ):
                raise JCalParsingError(
                    "Parameter values must be a string, integer or "
                    "float or a list of those.",
                    cls,
                    name,
                    value=value,
                )
        return cls(jcal)

    @classmethod
    def from_jcal_property(cls, jcal_property: list):
        """Create the parameters for a jCal property.

        Parameters:
            jcal_property (list): The jCal property [name, params, value, ...]
            default_value (str, optional): The default value of the property.
                If this is given, the default value will not be set.
        """
        if not isinstance(jcal_property, list) or len(jcal_property) < 4:
            raise JCalParsingError(
                "The property must be a list with at least 4 items.", cls
            )
        jcal_params = jcal_property[1]
        with JCalParsingError.reraise_with_path_added(1):
            self = cls.from_jcal(jcal_params)
        if self.is_utc():
            del self.tzid  # we do not want this parameter
        return self


RFC_6868_UNESCAPE_REGEX = re.compile(r"\^\^|\^n|\^'")


def rfc_6868_unescape(param_value: str) -> str:
    """Take care of :rfc:`6868` unescaping.

    - ^^ -> ^
    - ^n -> system specific newline
    - ^' -> "
    - ^ with others stay intact
    """
    replacements = {
        "^^": "^",
        "^n": os.linesep,
        "^'": '"',
    }
    return RFC_6868_UNESCAPE_REGEX.sub(
        lambda m: replacements.get(m.group(0), m.group(0)), param_value
    )


RFC_6868_ESCAPE_REGEX = re.compile(r'\^|\r\n|\r|\n|"')


def rfc_6868_escape(param_value: str) -> str:
    """Take care of :rfc:`6868` escaping.

    - ^ -> ^^
    - " -> ^'
    - newline -> ^n
    """
    replacements = {
        "^": "^^",
        "\n": "^n",
        "\r": "^n",
        "\r\n": "^n",
        '"': "^'",
    }
    return RFC_6868_ESCAPE_REGEX.sub(
        lambda m: replacements.get(m.group(0), m.group(0)), param_value
    )


__all__ = [
    "Parameters",
    "dquote",
    "param_value",
    "q_join",
    "q_split",
    "rfc_6868_escape",
    "rfc_6868_unescape",
    "validate_param_value",
]
