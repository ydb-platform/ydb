"""
Configuration utilities copied from spacy.util.
"""
import sys
from typing import Dict, Any, Tuple, Callable, Iterator, List, Optional, IO
import re

from spacy import Language
from spacy.util import registry


LoggerT = Callable[
    [Language, IO, IO],
    Tuple[Callable[[Optional[Dict[str, Any]]], None], Callable[[], None]],
]


def walk_dict(
    node: Dict[str, Any], parent: List[str] = []
) -> Iterator[Tuple[List[str], Any]]:
    """Walk a dict and yield the path and values of the leaves."""
    for key, value in node.items():
        key_parent = [*parent, key]
        if isinstance(value, dict):
            yield from walk_dict(value, key_parent)
        else:
            yield (key_parent, value)


def dot_to_dict(values: Dict[str, Any]) -> Dict[str, dict]:
    """Convert dot notation to a dict. For example: {"token.pos": True,
    "token._.xyz": True} becomes {"token": {"pos": True, "_": {"xyz": True }}}.

    values (Dict[str, Any]): The key/value pairs to convert.
    RETURNS (Dict[str, dict]): The converted values.
    """
    result = {}
    for key, value in values.items():
        path = result
        parts = key.lower().split(".")
        for i, item in enumerate(parts):
            is_last = i == len(parts) - 1
            path = path.setdefault(item, value if is_last else {})
    return result


def dict_to_dot(obj: Dict[str, dict]) -> Dict[str, Any]:
    """Convert dot notation to a dict. For example: {"token": {"pos": True,
    "_": {"xyz": True }}} becomes {"token.pos": True, "token._.xyz": True}.

    values (Dict[str, dict]): The dict to convert.
    RETURNS (Dict[str, Any]): The key/value pairs.
    """
    return {".".join(key): value for key, value in walk_dict(obj)}


def matcher_for_regex_patterns(
    regexps: Optional[List[str]] = None,
) -> Callable[[str], bool]:
    try:
        compiled = []
        if regexps is not None:
            for regex in regexps:
                compiled.append(re.compile(regex, flags=re.MULTILINE))
    except re.error as err:
        raise ValueError(
            f"Regular expression `{regex}` couldn't be compiled for logger stats matcher"
        ) from err

    def is_match(string: str) -> bool:
        for regex in compiled:
            if regex.search(string):
                return True
        return False

    return is_match


def setup_default_console_logger(
    nlp: "Language", stdout: IO = sys.stdout, stderr: IO = sys.stderr
) -> Tuple[Callable, Callable]:
    console_logger = registry.get("loggers", "spacy.ConsoleLogger.v1")
    console = console_logger(progress_bar=False)
    console_log_step, console_finalize = console(nlp, stdout, stderr)
    return console_log_step, console_finalize
