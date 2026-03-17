# encoding: utf-8
import sys

from mo_future import text

from mo_parsing.core import ParserElement
from mo_parsing.utils import (
    quote,
    lineno,
    col,
    stack_depth,
    quote as plain_quote,
    ParseException,
)

DEBUGGER = None
_max_preamble = 60


class Debugger(object):
    def __init__(self, silent=False):
        self.previous_parse = None
        self.was_debugging = None
        self.parse_count = 0
        self.max_stack_depth = 0
        self.silent = silent

    def __enter__(self):
        global DEBUGGER
        self.was_debugging = DEBUGGER
        DEBUGGER = self
        self.previous_parse = ParserElement._parse
        ParserElement._parse = _debug_parse(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global DEBUGGER
        ParserElement._parse = self.previous_parse
        DEBUGGER = self.was_debugging


def _debug_parse(debugger):
    def debug_parse(self, string, start, do_actions=True):
        if not debugger.silent:
            _try(self, start, string)
        loc = start
        try:
            debugger.parse_count += 1
            debugger.max_stack_depth = stackdepth()
            tokens = self.parse_impl(string, loc, do_actions)
        except ParseException as cause:
            self.parser_config.fail_action and self.parser_config.fail_action(
                self, start, string, cause
            )
            if not debugger.silent:
                fail(self, start, string, cause)
            raise ParseException(self, start, string, cause=cause) from None

        if self.parse_action and (do_actions or self.parser_config.callDuringTry):
            try:
                for fn in self.parse_action:
                    tokens = fn(tokens, start, string)
            except Exception as cause:
                fail(self, start, string, cause)
                raise
        if not debugger.silent:
            match(self, loc, tokens.end, string, tokens)

        return tokens

    return debug_parse


def _try(expr, start, string):
    global _max_preamble
    preamble = f"  Attempt {quote(string, start)} at loc ({lineno(start, string)},{col(start, string)}), index={start}"
    length = len(preamble)
    _max_preamble = max(_max_preamble, length)
    print(
        preamble
        + " " * (_max_preamble - length)
        + "for"
        + " " * stack_depth()
        + text(expr)[:300]
    )


def match(expr, start, end, string, tokens):
    global _max_preamble
    preamble = f"> Matched {quote(string[start:end])} at loc ({lineno(start, string)},{col(start, string)}), length={end-start}"
    length = len(preamble)
    _max_preamble = max(_max_preamble, length)
    print(
        preamble
        + " " * (_max_preamble - length)
        + "for"
        + " " * stack_depth()
        + f"{expr} -> {tokens}"
    )


def fail(expr, start, string, cause):
    quoted = plain_quote(text(cause))
    print("  Except  " + quoted)


def quote(value, start=0, length=12):
    return (plain_quote(value[start : start + length - 2]) + (" " * length))[:length]


def stackdepth():
    """
    return depth of stack at call point
    """
    frame = sys._getframe(1)
    depth = 0
    while frame:
        depth += 1
        frame = frame.f_back
    return depth
