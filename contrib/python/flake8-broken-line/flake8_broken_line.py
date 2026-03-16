import re
import tokenize
from typing import Iterator, Sequence, Set, Tuple
from importlib import metadata as importlib_metadata

#: This is a name that we use to install this library:
pkg_name = 'flake8-broken-line'

#: We store the version number inside the `pyproject.toml`:
pkg_version = importlib_metadata.version(pkg_name)

_INVALID_LINE_BREAK = re.compile(r'(?<!\\)\\$', re.M)
_INVALID_MULTILINE_BACKSLASH = re.compile(r'(?<![\']{3}|[\"]{3})\\$', re.M)
_CONDITIONS = (
    _INVALID_LINE_BREAK,
    _INVALID_MULTILINE_BACKSLASH,
)
_IGNORED_TOKENS = frozenset((
    tokenize.STRING,
    tokenize.COMMENT,
    tokenize.NL,
    tokenize.NEWLINE,
    tokenize.ENDMARKER,
))

_N400 = 'N400: Found backslash that is used for line breaking'


def check_line_breaks(
    tree,  # we only need this parameter, so plugin will be installed.
    file_tokens: Sequence[tokenize.TokenInfo],
) -> Iterator[Tuple[int, int, str, str]]:
    """Functional ``flake8`` plugin to check for backslashes."""
    reported: Set[int] = set()

    for line_token in file_tokens:
        if line_token.exact_type in _IGNORED_TOKENS:
            continue

        if line_token.start[0] in reported:
            continue  # There might be several tokens on a same line.

        if all(condition.search(line_token.line) for condition in _CONDITIONS):
            yield (*line_token.start, _N400, 'check_line_breaks')
            reported.add(line_token.start[0])


# Flake8 API definition:
check_line_breaks.name = pkg_name  # type: ignore
check_line_breaks.version = pkg_version  # type: ignore
