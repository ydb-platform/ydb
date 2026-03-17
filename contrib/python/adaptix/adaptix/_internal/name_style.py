import re
from dataclasses import dataclass
from enum import Enum
from functools import partial
from typing import Callable


class NameStyle(Enum):
    """An enumeration of different naming conventions"""

    LOWER_SNAKE = "lower_snake"
    CAMEL_SNAKE = "camel_Snake"
    PASCAL_SNAKE = "Pascal_Snake"
    UPPER_SNAKE = "UPPER_SNAKE"

    LOWER_KEBAB = "lower-kebab"
    CAMEL_KEBAB = "camel-Kebab"
    PASCAL_KEBAB = "Pascal-Kebab"
    UPPER_KEBAB = "UPPER-KEBAB"

    LOWER = "lowercase"
    CAMEL = "camelCase"
    PASCAL = "PascalCase"
    UPPER = "UPPERCASE"

    LOWER_DOT = "lower.dot"
    CAMEL_DOT = "camel.Dot"
    PASCAL_DOT = "Pascal.Dot"
    UPPER_DOT = "UPPER.DOT"


@dataclass(frozen=True)
class StyleConversion:
    sep: str
    first: Callable[[str], str]
    other: Callable[[str], str]


LOWER_CASE = (str.lower, str.lower)
CAMEL_CASE = (str.lower, str.title)
PASCAL_CASE = (str.title, str.title)
UPPER_CASE = (str.upper, str.upper)

STYLE_CONVERSIONS = {
    NameStyle.LOWER_SNAKE: StyleConversion("_", *LOWER_CASE),
    NameStyle.CAMEL_SNAKE: StyleConversion("_", *CAMEL_CASE),
    NameStyle.PASCAL_SNAKE: StyleConversion("_", *PASCAL_CASE),
    NameStyle.UPPER_SNAKE: StyleConversion("_", *UPPER_CASE),

    NameStyle.LOWER_KEBAB: StyleConversion("-", *LOWER_CASE),
    NameStyle.CAMEL_KEBAB: StyleConversion("-", *CAMEL_CASE),
    NameStyle.PASCAL_KEBAB: StyleConversion("-", *PASCAL_CASE),
    NameStyle.UPPER_KEBAB: StyleConversion("-", *UPPER_CASE),

    NameStyle.LOWER: StyleConversion("", *LOWER_CASE),
    NameStyle.CAMEL: StyleConversion("", *CAMEL_CASE),
    NameStyle.PASCAL: StyleConversion("", *PASCAL_CASE),
    NameStyle.UPPER: StyleConversion("", *UPPER_CASE),

    NameStyle.LOWER_DOT: StyleConversion(".", *LOWER_CASE),
    NameStyle.CAMEL_DOT: StyleConversion(".", *CAMEL_CASE),
    NameStyle.PASCAL_DOT: StyleConversion(".", *PASCAL_CASE),
    NameStyle.UPPER_DOT: StyleConversion(".", *UPPER_CASE),
}

ONLY_WORD_CHARS = re.compile(r"\w+")


def is_snake_style(name: str) -> bool:
    return ONLY_WORD_CHARS.fullmatch(name) is not None


SNAKE_SPLITTER = re.compile(r"(_*)([^_]+)(.*?)(_*)$")
REST_SUB = re.compile(r"(_+)|([^_]+)")


def rest_sub(conv: StyleConversion, match_: re.Match):
    if match_[1] is None:
        return conv.other(match_[2])
    return match_[1].replace("_", conv.sep)


def convert_snake_style(name: str, style: NameStyle) -> str:
    if not is_snake_style(name):
        raise ValueError("Cannot convert a name that not follows snake style")

    match = SNAKE_SPLITTER.match(name)
    if match is None:
        raise ValueError(f"Cannot convert {name!r}")

    front_us, raw_first, raw_rest, trailing_us = match.groups()
    conv = STYLE_CONVERSIONS[style]

    first = conv.first(raw_first)
    rest = REST_SUB.sub(partial(rest_sub, conv), raw_rest)

    return front_us + first + rest + trailing_us
