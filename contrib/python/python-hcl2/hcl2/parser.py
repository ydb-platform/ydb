"""A parser for HCL2 implemented using the Lark parser"""
import functools
from pathlib import Path

from lark import Lark


PARSER_FILE = Path(__file__).absolute().resolve().parent / ".lark_cache.bin"


@functools.lru_cache()
def parser() -> Lark:
    """Build standard parser for transforming HCL2 text into python structures"""
    return Lark.open_from_package(
        __package__,
        "hcl2.lark",
        parser="lalr",
        cache=str(PARSER_FILE),  # Disable/Delete file to effect changes to the grammar
        propagate_positions=True,
    )


@functools.lru_cache()
def reconstruction_parser() -> Lark:
    """
    Build parser for transforming python structures into HCL2 text.
    This is duplicated from `parser` because we need different options here for
    the reconstructor. Please make sure changes are kept in sync between the two
    if necessary.
    """
    return Lark.open_from_package(
        __package__,
        "hcl2.lark",
        parser="lalr",
        # Caching must be disabled to allow for reconstruction until lark-parser/lark#1472 is fixed:
        #
        #   https://github.com/lark-parser/lark/issues/1472
        #
        # cache=str(PARSER_FILE),  # Disable/Delete file to effect changes to the grammar
        propagate_positions=True,
        maybe_placeholders=False,  # Needed for reconstruction
    )
