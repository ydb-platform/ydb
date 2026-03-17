"""Main parsing and formatting functionality."""

from __future__ import annotations

import re
import datetime as dt
import random
import string
from functools import lru_cache
import typing

if typing.TYPE_CHECKING:
    from _typeshed import StrOrLiteralStr
    from typing import Any
    from collections.abc import Iterable, Sequence, Mapping


class Parser:
    """Class-based interface to parsing and formatting functionality."""

    def __init__(self, fmt: str):
        self.fmt = fmt

    def __str__(self):
        return self.fmt

    def keys(self):
        """Get parameter names defined in the format string."""
        convert_dict = get_convert_dict(self.fmt)
        return convert_dict.keys()

    def parse(self, stri: str, full_match: bool = True) -> dict[str, Any]:
        """Parse keys and values from ``stri`` using parser's format."""
        return parse(self.fmt, stri, full_match=full_match)

    def compose(self, keyvals: Mapping[str, Any], allow_partial: bool = False) -> str:
        """Compose format string ``self.fmt`` with parameters given in the ``keyvals`` dict.

        Args:
            keyvals: "Parameter --> parameter value" map
            allow_partial: If True, then partial composition is allowed, i.e.,
                not all parameters present in `fmt` need to be specified in `keyvals`.
                Unspecified parameters will, in this case, be left unchanged.
                (Default value = False).

        Returns:
            Result of formatting the *self.fmt* string with parameter values
            extracted from the corresponding items in the *keyvals* dictionary.

        """
        return compose(fmt=self.fmt, keyvals=keyvals, allow_partial=allow_partial)

    format = compose

    def globify(self, keyvals: Mapping[str, Any] | None = None) -> str:
        """Generate a string usable with glob.glob() from format string."""
        return globify(self.fmt, keyvals)

    def validate(self, stri: str) -> bool:
        """Validate that string ``stri`` conforms to the parser's format definition.

        Checks that the provided string is parsable and therefore complies with
        this parser's string format definition.  Useful for filtering strings,
        or to check if a string is compatible before passing it to the
        parser function.
        """
        return validate(self.fmt, stri)

    def is_one2one(self):
        """Check if this parser's format string has a one to one correspondence.

        That is, that successive composing and
        parsing operations will result in the original data.
        In other words, that input data maps to a string,
        which then maps back to the original data without any change
        or loss in information.

        Note: This test only applies to sensible usage of the format string.
        If string or numeric data causes overflow, e.g.
        if composing "abcd" into ``{3s}``, one to one correspondence will always
        be broken in such cases. This of course also applies to precision
        losses when using datetime data.
        """
        return is_one2one(self.fmt)


class StringFormatter(string.Formatter):
    """Custom string formatter class for basic strings.

    This formatter adds a few special conversions for assisting with common
    trollsift situations like making a parameter lowercase or removing
    hyphens. The added conversions are listed below and can be used in a
    format string by prefixing them with an `!` like so:

    >>> fstr = "{!u}_{!l}"
    >>> formatter = StringFormatter()
    >>> formatter.format(fstr, "to_upper", "To_LowerCase")
    "TO_UPPER_to_lowercase"

    - c: Make capitalized version of string (first character upper case, all lowercase after that) by executing the
      parameter's `.capitalize()` method.
    - l: Make all characters lowercase by executing the parameter's `.lower()` method.
    - R: Remove all separators from the parameter including '-', '_', ' ', and ':'.
    - t: Title case the string by executing the parameter's `.title()` method.
    - u: Make all characters uppercase by executing the parameter's `.upper()` method.
    - h: A combination of 'R' and 'l'.
    - H: A combination of 'R' and 'u'.

    """

    CONV_FUNCS = {
        "c": "capitalize",
        "h": "lower",
        "H": "upper",
        "l": "lower",
        "t": "title",
        "u": "upper",
    }

    def convert_field(self, value: str, conversion: str | None) -> str:
        """Apply conversions mentioned in `StringFormatter.CONV_FUNCS`."""
        if conversion is None:
            func = None
        else:
            func = self.CONV_FUNCS.get(conversion)
        if func is not None:
            value = getattr(value, func)()
        elif conversion not in ["R"]:
            # default conversion ('r', 's')
            return super(StringFormatter, self).convert_field(value, conversion)

        if conversion in ["h", "H", "R"]:
            value = value.replace("-", "").replace("_", "").replace(":", "").replace(" ", "")
        return value


formatter = StringFormatter()


# taken from https://docs.python.org/3/library/re.html#simulating-scanf
spec_regexes = {
    "b": r"[-+]?[0-1]",
    "c": r".",
    "d": r"[-+]?\d",
    # Naive fixed point format specifier (e.g. {foo:f})
    "f": r"[-+]?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?",
    # Fixed point format specifier including width and precision
    # (e.g. {foo:4.2f}). The lookahead (?=.{width}) makes sure that the
    # subsequent pattern is only matched if the string has the required
    # (minimum) width.
    "f_with_precision": r"(?=.{{{width}}})([-+]?([\d ]+(\.\d{{{decimals}}})+|\.\d{{{decimals}}})([eE][-+]?\d+)?)",
    "i": r"[-+]?(0[xX][\dA-Fa-f]+|0[0-7]*|\d+)",
    "o": r"[-+]?[0-7]",
    "s": r"\S",
    "x": r"[-+]?(0[xX])?[\dA-Fa-f]",
}
spec_regexes["e"] = spec_regexes["f"]
spec_regexes["E"] = spec_regexes["f"]
spec_regexes["g"] = spec_regexes["f"]
spec_regexes["X"] = spec_regexes["x"]
spec_regexes[""] = spec_regexes["s"]
allow_multiple = ["b", "c", "d", "o", "s", "", "x", "X"]
fixed_point_types = ["f", "e", "E", "g"]
# format_spec ::=  [[fill]align][sign][#][0][width][,][.precision][type]
# https://docs.python.org/3.4/library/string.html#format-specification-mini-language
fmt_spec_regex = re.compile(
    r"(?P<align>(?P<fill>.)?[<>=^])?(?P<sign>[\+\-\s])?(?P<pound>#)?(?P<zero>0)?(?P<width>\d+)?"
    r"(?P<comma>,)?(?P<precision>.\d+)?(?P<type>[bcdeEfFgGnosxX%]?)"
)


def _get_fixed_point_regex(width: str | None, precision: str | None) -> str:
    """Get regular expression for fixed point numbers.

    Args:
        width: Total width of the string representation.
        precision: Number of decimals.
    """
    if width or precision:
        if precision is None:
            precision = "0,"
        else:
            precision = precision.strip(".")
        if width is None:
            width = "1,"
        return spec_regexes["f_with_precision"].format(width=width, decimals=precision)
    else:
        return spec_regexes["f"]


class RegexFormatter(string.Formatter):
    """String formatter that converts a format string to a regular expression.

    >>> regex_formatter = RegexFormatter()
    >>> regex_str = regex_formatter.format('{field_one:5d}_{field_two}')

    Can also be used to extract values from a string given the format spec
    for that string:

    >>> regex_formatter.extract_values('{field_one:5d}_{field_two}', '12345_sometext')
    {'field_one': '12345', 'field_two': 'sometext'}

    Note that the regular expressions generated by this class are specially
    generated to reduce "greediness" of the matches found. For ambiguous
    patterns where a single field could match shorter or longer portions of
    the provided string, this class will prefer the shorter version of the
    string in order to make the rest of the pattern match. For example:

    >>> regex_formatter.extract_values('{field_one}_{field_two}', 'abc_def_ghi')
    {'field_one': 'abc', 'field_two': 'def_ghi'}

    Note how `field_one` could have matched "abc_def", but the lower
    greediness of this parser caused it to only match against "abc".

    """

    # special string to mark a parameter not being specified
    UNPROVIDED_VALUE = "<trollsift unprovided value>"
    ESCAPE_CHARACTERS = ["\\"] + [x for x in string.punctuation if x not in "\\%"]
    ESCAPE_SETS = [(c, "\\" + c) for c in ESCAPE_CHARACTERS]

    def __init__(self):
        # hold on to fields we've seen already so we can reuse their
        # definitions in the regex
        self._cached_fields = {}
        self.format = lru_cache()(self._uncached_format)
        super(RegexFormatter, self).__init__()

    def _uncached_format(*args, **kwargs):
        try:
            # super() doesn't seem to work here
            ret_val = string.Formatter.format(*args, **kwargs)
        finally:
            self = args[0]  # just matching the parent class
            self._cached_fields.clear()
        return ret_val

    def _escape(self, s: str) -> str:
        """Escape bad characters for regular expressions.

        Similar to `re.escape` but allows '%' to pass through.

        """
        for ch, r_ch in self.ESCAPE_SETS:
            s = s.replace(ch, r_ch)
        return s

    def parse(
        self, format_string: StrOrLiteralStr
    ) -> Iterable[
        tuple[
            StrOrLiteralStr,
            StrOrLiteralStr | None,
            StrOrLiteralStr | None,
            StrOrLiteralStr | None,
        ]
    ]:
        parse_ret = super(RegexFormatter, self).parse(format_string)
        for literal_text, field_name, format_spec, conversion in parse_ret:
            # the parent class will call parse multiple times moving
            # 'format_spec' to 'literal_text'. We only escape 'literal_text'
            # so we don't escape things twice.
            literal_text = self._escape(literal_text)
            yield literal_text, field_name, format_spec, conversion

    def get_value(self, key: int | str, args: Sequence[Any], kwargs: Mapping[str, Any]) -> Any:
        try:
            return super(RegexFormatter, self).get_value(key, args, kwargs)
        except (IndexError, KeyError):
            return key, self.UNPROVIDED_VALUE

    def _regex_datetime(self, format_spec: str) -> str:
        replace_str = format_spec
        for fmt_key, fmt_val in DT_FMT.items():
            if fmt_key == "%%":
                # special case
                replace_str.replace("%%", "%")
                continue
            count = fmt_val.count("?")
            # either a series of numbers or letters/numbers
            regex = r"\d{{{:d}}}".format(count) if count else r"[^ \t\n\r\f\v\-_:]+"
            replace_str = replace_str.replace(fmt_key, regex)
        return replace_str

    def regex_field(self, field_name: str, value: Any, format_spec: str) -> str:
        if value != self.UNPROVIDED_VALUE:
            return super(RegexFormatter, self).format_field(value, format_spec)

        if self._cached_fields.get(field_name, format_spec) != format_spec:
            raise ValueError("Can't specify the same field_name with different formats: {}".format(field_name))
        elif field_name in self._cached_fields:
            return r"(?P={})".format(field_name)
        else:
            self._cached_fields[field_name] = format_spec

        # Replace format spec with glob patterns (*, ?, etc)
        if not format_spec:
            return r"(?P<{}>.*?)".format(field_name)
        if "%" in format_spec:
            return r"(?P<{}>{})".format(field_name, self._regex_datetime(format_spec))
        return format_spec_to_regex(field_name, format_spec)

    def format_field(self, value: Any, format_spec: str) -> str:
        if not isinstance(value, tuple) or value[1] != self.UNPROVIDED_VALUE:
            return super(RegexFormatter, self).format_field(value, format_spec)
        field_name, value = value
        return self.regex_field(field_name, value, format_spec)


def format_spec_to_regex(field_name: str, format_spec: str) -> str:
    """Make an attempt at converting a format spec to a regular expression."""
    # NOTE: remove escaped backslashes so regex matches
    regex_match = fmt_spec_regex.match(format_spec.replace("\\", ""))
    if regex_match is None:
        raise ValueError("Invalid format specification: '{}'".format(format_spec))
    regex_dict = regex_match.groupdict()
    ftype = regex_dict["type"]
    width = regex_dict["width"]
    align = regex_dict["align"]
    precision = regex_dict["precision"]
    fill = _get_fill(regex_dict["fill"], width, ftype)

    char_type = spec_regexes[ftype]
    if ftype in fixed_point_types:
        char_type = _get_fixed_point_regex(width=width, precision=precision)
    if ftype in ("s", "") and align and align.endswith("="):
        raise ValueError("Invalid format specification: '{}'".format(format_spec))
    final_regex = char_type
    if ftype in allow_multiple and (not width or width == "0"):
        final_regex += r"*?"
    elif width and width != "0":
        if not fill and ftype not in fixed_point_types:
            # we know we have exactly this many characters
            final_regex += r"{{{}}}".format(int(width))
        elif fill:
            # we don't know how many fill characters we have compared to
            # field characters so just match all characters and sort it out
            # later during type conversion.
            final_regex = r".{{{}}}".format(int(width))
        elif ftype in allow_multiple:
            final_regex += r"*?"

    return r"(?P<{}>{})".format(field_name, final_regex)


def _get_fill(fill: str | None, width: str | None, ftype: str | None) -> str | None:
    # NOTE: does not properly handle `=` alignment
    if fill is None:
        if width is not None and width[0] == "0":
            fill = "0"
        elif ftype in ["s", "", "d", "x", "X", "o", "b"]:
            fill = " "
    return fill


@lru_cache()
def regex_format(fmt: str) -> str:
    # We create a new instance of RegexFormatter here to prevent concurrent calls to
    # format interfering with one another.
    return RegexFormatter().format(fmt)


def extract_values(fmt: str, stri: str, full_match: bool = True) -> dict[str, Any]:
    """Extract information from string matching format.

    Args:
        fmt: Python format string to match against
        stri: String to extract information from
        full_match: Force the match of the whole string. Default
            to ``True``.
    """
    regex = regex_format(fmt)
    if full_match:
        regex = "^" + regex + "$"
    match = re.match(regex, stri)
    if match is None:
        raise ValueError("String does not match pattern.")
    return match.groupdict()


def _get_number_from_fmt(fmt: str) -> int:
    """Helper function for extract_values.

    Figures out string length from format string.

    """
    if "%" in fmt:
        # its datetime
        return len(("{0:" + fmt + "}").format(dt.datetime.now()))
    else:
        # its something else
        fmt = fmt.lstrip("0")
        fmt_digits_match = re.search("[0-9]+", fmt)
        if fmt_digits_match is None:
            raise ValueError(f"No number specified in format string: {fmt}")
        return int(fmt_digits_match.group(0))


def _convert(convdef: str, stri: str) -> Any:
    """Convert the string *stri* to the given conversion definition *convdef*."""
    result: Any  # force mypy type
    if "%" in convdef:
        result = dt.datetime.strptime(stri, convdef)
    else:
        result = _strip_padding(convdef, stri)
        if "d" in convdef:
            result = int(result)
        elif "x" in convdef or "X" in convdef:
            result = int(result, 16)
        elif "o" in convdef:
            result = int(result, 8)
        elif "b" in convdef:
            result = int(result, 2)
        elif any(float_type_marker in convdef for float_type_marker in fixed_point_types):
            result = float(result)

    return result


def _strip_padding(convdef: str, stri: str) -> str:
    """Strip padding from the given string.

    Args:
        convdef: Conversion definition (indicates the padding)
        stri: String to be modified
    """
    regex_match = fmt_spec_regex.match(convdef)
    match_dict = regex_match.groupdict() if regex_match else {}
    align = match_dict.get("align")
    pad = match_dict.get("fill")
    if align:
        # align character is the last one
        align = align[-1]
    if align and align in "<>^" and not pad:
        pad = " "
    if align == ">":
        stri = stri.lstrip(pad)
    elif align == "<":
        stri = stri.rstrip(pad)
    elif align == "^":
        stri = stri.strip(pad)
    return stri


@lru_cache()
def get_convert_dict(fmt: str) -> dict[str, str]:
    """Retrieve parse definition from the format string `fmt`."""
    convdef = {}
    for _literal_text, field_name, format_spec, _conversion in formatter.parse(fmt):
        if field_name is None or format_spec is None:
            continue
        # XXX: Do I need to include 'conversion'?
        convdef[field_name] = format_spec
    return convdef


def parse(fmt: str, stri: str, full_match: bool = True) -> dict[str, Any]:
    """Parse keys and corresponding values from *stri* using format described in *fmt* string.

    Args:
        fmt: Python format string to match against
        stri: String to extract information from
        full_match: Force the match of the whole string. Default True.

    """
    convdef = get_convert_dict(fmt)
    keyvals = extract_values(fmt, stri, full_match=full_match)
    for key in convdef.keys():
        keyvals[key] = _convert(convdef[key], keyvals[key])

    return keyvals


def compose(fmt: str, keyvals: Mapping[str, Any], allow_partial: bool = False) -> str:
    """Compose format string *self.fmt* with parameters given in the *keyvals* dict.

    Args:
        fmt: Python format string to match against
        keyvals: "Parameter --> parameter value" map
        allow_partial: If True, then partial composition is allowed, i.e.,
            not all parameters present in `fmt` need to be specified in `keyvals`.
            Unspecified parameters will, in this case, be left unchanged.
            (Default value = False).

    Returns:
        Result of formatting the *self.fmt* string with parameter values
        extracted from the corresponding items in the *keyvals* dictionary.

    """
    if allow_partial:
        return _partial_compose(fmt=fmt, keyvals=keyvals)
    return _strict_compose(fmt=fmt, keyvals=keyvals)


DT_FMT = {
    "%a": "*",
    "%A": "*",
    "%w": "?",
    "%d": "??",
    "%b": "*",
    "%B": "*",
    "%m": "??",
    "%y": "??",
    "%Y": "????",
    "%H": "??",
    "%I": "??",
    "%p": "*",
    "%M": "??",
    "%S": "??",
    "%f": "*",
    "%z": "*",
    "%Z": "*",
    "%j": "???",
    "%U": "??",
    "%W": "??",
    "%c": "*",
    "%x": "*",
    "%X": "*",
    "%%": "?",
}


class GlobifyFormatter(string.Formatter):
    # special string to mark a parameter not being specified
    UNPROVIDED_VALUE = "<trollsift unprovided value>"

    def get_value(self, key: str | int, args: Sequence[Any], kwargs: Mapping[str, Any]) -> Any:
        try:
            return super(GlobifyFormatter, self).get_value(key, args, kwargs)
        except (IndexError, KeyError):
            # assumes that
            return self.UNPROVIDED_VALUE

    def format_field(self, value: Any, format_spec: str) -> str:
        if not isinstance(value, (list, tuple)) and value != self.UNPROVIDED_VALUE:
            return super(GlobifyFormatter, self).format_field(value, format_spec)
        elif value != self.UNPROVIDED_VALUE:
            # partial provided date/time fields
            # specified with a tuple/list of 2 elements
            # (value, partial format string)
            value, dt_fmt = value
            for fmt_letter in dt_fmt:
                fmt = "%" + fmt_letter
                format_spec = format_spec.replace(fmt, value.strftime(fmt))

        # Replace format spec with glob patterns (*, ?, etc)
        if not format_spec:
            return "*"
        if "%" in format_spec:
            replace_str = format_spec
            for fmt_key, fmt_val in DT_FMT.items():
                replace_str = replace_str.replace(fmt_key, fmt_val)
            return replace_str
        if not re.search("[0-9]+", format_spec):
            # non-integer type
            return "*"
        return "?" * _get_number_from_fmt(format_spec)


globify_formatter = GlobifyFormatter()


def globify(fmt: str, keyvals: Mapping[str, Any] | None = None) -> Any:
    """Generate a string usable with glob.glob() from format string and provided information."""
    if keyvals is None:
        keyvals = {}
    return globify_formatter.format(fmt, **keyvals)


def validate(fmt: str, stri: str) -> bool:
    """Validates that string ``stri`` conforms to ``fmt``.

    Useful for filtering string, or to check if string is compatible before
    passing the string to the parser function.
    """
    try:
        parse(fmt, stri)
        return True
    except ValueError:
        return False


def _generate_data_for_format(fmt: str) -> dict[str, Any]:
    """Generate a fake data dictionary to fill in the provided format string."""
    # finally try some data, create some random data for the fmt.
    data = {}
    # keep track of how many "free_size" (wildcard) parameters we have
    # if we get two in a row then we know the pattern is invalid, meaning
    # we'll never be able to match the second wildcard field
    free_size_start = False
    for literal_text, field_name, format_spec, _conversion in formatter.parse(fmt):
        if literal_text:
            free_size_start = False

        if not field_name:
            free_size_start = False
            continue

        # encapsulating free size keys,
        # e.g. {:s}{:s} or {:s}{:4s}{:d}
        if not format_spec or format_spec == "s" or format_spec == "d":
            if free_size_start:
                raise ValueError("Can't generate data for spec with two or more fields with no size specifier.")
            else:
                free_size_start = True

        # make some data for this key and format
        data[field_name] = _gen_data_for_spec(format_spec)
    return data


def _gen_data_for_spec(format_spec: str | None) -> int | str | dt.datetime:
    if format_spec and "%" in format_spec:
        # some datetime
        t = dt.datetime.now()
        # run once through format to limit precision
        t = parse("{t:" + format_spec + "}", compose("{t:" + format_spec + "}", {"t": t}))["t"]
        return t

    if format_spec and "d" in format_spec:
        # random number (with n sign. figures)
        if not format_spec.isalpha():
            n = _get_number_from_fmt(format_spec)
        else:
            # clearly bad
            raise ValueError(f"Bad format specification: {format_spec!r}")
        return random.randint(0, 99999999999999999) % (10**n)

    # string type
    if format_spec is None:
        n = 4
    elif format_spec.isalnum():
        n = _get_number_from_fmt(format_spec)
    else:
        n = 4
    randstri = ""
    for _ in range(n):
        randstri += random.choice(string.ascii_letters)
    return randstri


def is_one2one(fmt: str) -> bool:
    """Check if the format string has a one to one correspondence.

    That is, that successive composing and
    parsing operations will result in the original data.
    In other words, that input data maps to a string,
    which then maps back to the original data without any change
    or loss in information.

    Note: This test only applies to sensible usage of the format string.
    If string or numeric data is causes overflow, e.g.
    if composing "abcd" into {3s}, one to one correspondence will always
    be broken in such cases. This of course also applies to precision
    losses when using  datetime data.
    """
    try:
        data = _generate_data_for_format(fmt)
    except ValueError:
        return False

    # run data forward once and back to data
    stri = compose(fmt, data)
    data2 = parse(fmt, stri)
    # check if data2 equal to original data
    if len(data) != len(data2):
        return False
    for key in data:
        if key not in data2:
            return False
        if data2[key] != data[key]:
            return False
    # all checks passed, so just return True
    return True


def purge() -> None:
    """Clear internal caches.

    Not needed normally, but can be used to force cache clear when memory
    is very limited.

    """
    regex_format.cache_clear()
    get_convert_dict.cache_clear()


def _strict_compose(fmt: str, keyvals: Mapping[str, Any]) -> str:
    """Convert parameters in `keyvals` to a string based on `fmt` string."""
    return formatter.format(fmt, **keyvals)


def _partial_compose(fmt: str, keyvals: Mapping[str, Any]) -> str:
    """Convert parameters in `keyvals` to a string based on `fmt` string.

    Similar to _strict_compose, but accepts partial composing, i.e., not all
    parameters in `fmt` need to be specified in `keyvals`. Unspecified parameters
    are left unchanged.

    Args:
        fmt (str): Python format string to match against
        keyvals (dict): "Parameter --> parameter value" map

    """
    fmt, undefined_vars = _replace_undefined_params_with_placeholders(fmt, keyvals)
    composed_string = _strict_compose(fmt=fmt, keyvals=keyvals)
    for fmt_placeholder, fmt_specification in undefined_vars.items():
        composed_string = composed_string.replace(fmt_placeholder, fmt_specification)

    return composed_string


def _replace_undefined_params_with_placeholders(
    fmt: str, keyvals: Mapping[str, Any] | None = None
) -> tuple[str, dict[str, Any]]:
    """Replace with placeholders params in `fmt` not specified in `keyvals`."""
    vars_left_undefined = set(get_convert_dict(fmt).keys())
    if keyvals is not None:
        vars_left_undefined -= keyvals.keys()

    undefined_vars_placeholders_dict = {}
    new_fmt = fmt
    for var in sorted(vars_left_undefined):
        matches = set(match.group() for match in re.finditer(rf"{{{re.escape(var)}([^\w{{}}].*?)*}}", new_fmt))
        if len(matches) == 0:
            raise ValueError(f"Could not capture definitions for {var} from {fmt}")
        for var_specification in matches:
            fmt_placeholder = f"({hex(hash(var_specification))})"
            undefined_vars_placeholders_dict[fmt_placeholder] = var_specification
            new_fmt = new_fmt.replace(var_specification, fmt_placeholder)

    return new_fmt, undefined_vars_placeholders_dict
