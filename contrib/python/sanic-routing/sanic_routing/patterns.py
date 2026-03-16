import re
import typing as t
import uuid

from datetime import date, datetime
from types import SimpleNamespace
from typing import Any, Callable, Dict, Pattern, Tuple, Type

from sanic_routing.exceptions import InvalidUsage, NotFound


def parse_date(d) -> date:
    return datetime.strptime(d, "%Y-%m-%d").date()


def alpha(param: str) -> str:
    if not param.isalpha():
        raise ValueError(f"Value {param} contains non-alphabetic chracters")
    return param


def slug(param: str) -> str:
    if not REGEX_TYPES["slug"][1].match(param):
        raise ValueError(f"Value {param} does not match the slug format")
    return param


def ext(param: str) -> Tuple[str, ...]:
    parts = tuple(param.split("."))
    if any(not p for p in parts) or len(parts) == 1:
        raise ValueError(f"Value {param} does not match filename format")
    return parts


def nonemptystr(param: str) -> str:
    if not param:
        raise ValueError(f"Value {param} is an empty string")
    return param


class ParamInfo:
    __slots__ = (
        "cast",
        "ctx",
        "label",
        "name",
        "pattern",
        "priority",
        "raw_path",
        "regex",
    )

    def __init__(
        self,
        name: str,
        raw_path: str,
        label: str,
        cast: t.Callable[[str], t.Any],
        pattern: re.Pattern,
        regex: bool,
        priority: int,
    ) -> None:
        self.name = name
        self.raw_path = raw_path
        self.label = label
        self.cast = cast
        self.pattern = pattern
        self.regex = regex
        self.priority = priority
        self.ctx = SimpleNamespace()

    def process(
        self,
        params: t.Dict[str, t.Any],
        value: t.Union[str, t.Tuple[str, ...]],
    ) -> None:
        params[self.name] = value


class ExtParamInfo(ParamInfo):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        match = REGEX_PARAM_EXT_PATH.search(self.raw_path)
        if not match:
            raise InvalidUsage(
                f"Invalid extension parameter definition: {self.raw_path}"
            )
        if match.group(2) == "path":
            raise InvalidUsage(
                "Extension parameter matching does not support the "
                "`path` type."
            )
        ext_type = match.group(3)
        regex_type = REGEX_TYPES.get(match.group(2))
        self.ctx.cast = None
        if regex_type:
            self.ctx.cast = regex_type[0]
        elif match.group(2):
            raise InvalidUsage(
                "Extension parameter matching only supports filename matching "
                "on known parameter types, and not regular expressions."
            )
        self.ctx.allowed = []
        self.ctx.allowed_sub_count = 0
        if ext_type:
            self.ctx.allowed = ext_type.split("|")
            allowed_subs = {allowed.count(".") for allowed in self.ctx.allowed}
            if len(allowed_subs) > 1:
                raise InvalidUsage(
                    "All allowed extensions within a single route definition "
                    "must contain the same number of subparts. For example: "
                    "<foo:ext=js|css> and <foo:ext=min.js|min.css> are both "
                    "acceptable, but <foo:ext=js|min.js> is not."
                )
            self.ctx.allowed_sub_count = next(iter(allowed_subs))

            for extension in self.ctx.allowed:
                if not REGEX_ALLOWED_EXTENSION.match(extension):
                    raise InvalidUsage(f"Invalid extension: {extension}")

    def process(self, params, value):
        stop = -1 * (self.ctx.allowed_sub_count + 1)
        filename = ".".join(value[:stop])
        ext = ".".join(value[stop:])
        if self.ctx.allowed and ext not in self.ctx.allowed:
            raise NotFound(f"Invalid extension: {ext}")
        if self.ctx.cast:
            try:
                filename = self.ctx.cast(filename)
            except ValueError:
                raise NotFound(f"Invalid filename: {filename}")
        params[self.name] = filename
        params["ext"] = ext


EXTENSION = r"[a-z0-9](?:[a-z0-9\.]*[a-z0-9])?"
PARAM_EXT = (
    r"<([a-zA-Z_][a-zA-Z0-9_]*)(?:=([a-z]+))?(?::ext(?:=([a-z0-9|\.]+))?)>"
)
REGEX_PARAM_NAME = re.compile(r"^<([a-zA-Z_][a-zA-Z0-9_]*)(?::(.*))?>$")
REGEX_PARAM_EXT_PATH = re.compile(PARAM_EXT)
REGEX_PARAM_NAME_EXT = re.compile(r"^" + PARAM_EXT + r"$")
REGEX_ALLOWED_EXTENSION = re.compile(r"^" + EXTENSION + r"$")

# Predefined path parameter types. The value is a tuple consisteing of a
# callable and a compiled regular expression.
# The callable should:
#   1. accept a string input
#   2. cast the string to desired type
#   3. raise ValueError if it cannot
# The regular expression is generally NOT used. Unless the path is forced
# to use regex patterns.
REGEX_TYPES_ANNOTATION = Dict[
    str, Tuple[Callable[[str], Any], Pattern, Type[ParamInfo]]
]
REGEX_TYPES: REGEX_TYPES_ANNOTATION = {
    "strorempty": (str, re.compile(r"^[^/]*$"), ParamInfo),
    "str": (nonemptystr, re.compile(r"^[^/]+$"), ParamInfo),
    "ext": (ext, re.compile(r"^[^/]+\." + EXTENSION + r"$"), ExtParamInfo),
    "slug": (slug, re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$"), ParamInfo),
    "alpha": (alpha, re.compile(r"^[A-Za-z]+$"), ParamInfo),
    "path": (str, re.compile(r"^[^/]?.*?$"), ParamInfo),
    "float": (float, re.compile(r"^-?(?:\d+(?:\.\d*)?|\.\d+)$"), ParamInfo),
    "int": (int, re.compile(r"^-?\d+$"), ParamInfo),
    "ymd": (
        parse_date,
        re.compile(r"^([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))$"),
        ParamInfo,
    ),
    "uuid": (
        uuid.UUID,
        re.compile(
            r"^[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-"
            r"[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}$"
        ),
        ParamInfo,
    ),
}
