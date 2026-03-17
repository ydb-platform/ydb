# encoding: utf-8
import re
from collections import namedtuple

from mo_future import is_text
from mo_imports import expect, Expecting

from mo_parsing.core import ParserElement
from mo_parsing.results import ParseResults
from mo_parsing.utils import Log, indent, quote, regex_range, alphanums, regex_iso

Literal, Token, Empty = expect("Literal", "Token", "Empty")

CURRENT = None  # THE CURRENT DEFINED WHITESPACE
NO_WHITESPACE = None  # NOTHING IS WHITESPACE ENGINE
STANDARD_WHITESPACE = None  # SIMPLE WHITESPACE
whitespace_stack = []


class Whitespace(ParserElement):
    def __init__(self, white=" \n\r\t"):
        self.id = id(self)
        self.literal = Literal
        self.keyword_chars = alphanums + "_$"
        self.ignore_list = []
        self.debug_actions = DebugActions(noop, noop, noop)
        self.all_exceptions = {}
        self.content = None
        self.skips = {}
        self.regex = None
        self.expr = None
        self.parent = None
        self.copies = []
        self._in_regex = False
        self.set_whitespace(white)

    def copy(self):
        output = Whitespace(self.white_chars)
        output.id = self.id
        output.literal = self.literal
        output.keyword_chars = self.keyword_chars
        output.ignore_list = list(self.ignore_list)
        output.debug_actions = self.debug_actions
        output.all_exceptions = self.all_exceptions
        output.content = None
        output.skips = {}
        output.regex = self.regex
        output.expr = self.expr
        output.parent = self
        output.copies = []
        return output

    def __enter__(self):
        global CURRENT

        whitespace_stack.append(CURRENT)
        CURRENT = new_whitespace = self.copy()
        self.copies.append(new_whitespace)
        return new_whitespace

    use = __enter__

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        REMOVE THIS WHITESPACE CONTEXT
        """
        global CURRENT
        if self.copies and self.copies[-1] is CURRENT:
            self.copies.pop()
            CURRENT = whitespace_stack.pop()
            return

        if self.parent:
            self.parent.__exit__(exc_type, exc_val, exc_tb)
            self.parent = None
            return

        Log.error("Released wrong whitespace")

    def release(self):
        self.__exit__(None, None, None)

    def normalize(self, expr):
        if expr == None:
            return None
        if is_text(expr):
            return self.literal(expr)
        if isinstance(expr, type) and issubclass(expr, ParserElement):
            return expr()  # ALLOW Empty WHEN Empty() WAS INTENDED
        if not isinstance(expr, ParserElement):
            Log.error("expecting string, or ParserElemenet")

        return expr

    def record_exception(self, string, loc, expr, exc):
        es = self.all_exceptions.setdefault(loc, [])
        es.append(exc)

    def set_literal(self, literal):
        self.id = id(self)
        self.literal = literal

    def set_keyword_chars(self, chars):
        self.id = id(self)
        self.keyword_chars = "".join(sorted(set(chars)))

    def set_whitespace(self, chars):
        self.id = id(self)
        self.white_chars = "".join(sorted(set(chars)))
        self.content = None
        self.expr = None if isinstance(Empty, Expecting) else Empty()
        self.regex = re.compile(self.__regex__()[1], re.DOTALL)

    def add_ignore(self, *ignore_exprs):
        """
        ADD TO THE LIST OF IGNORED EXPRESSIONS
        :param ignore_expr:
        """
        self.id = id(self)
        for ignore_expr in ignore_exprs:
            ignore_expr = ignore_expr.suppress()
            self.ignore_list.append(ignore_expr)
            self.content = None
            self.expr = None if isinstance(Empty, Expecting) else Empty()
            self.regex = re.compile(self.__regex__()[1], re.DOTALL)
            return self

    def backup(self):
        return Backup(self)

    def parse_impl(self, string, start, do_actions=True):
        end = self.skip(string, start)
        return ParseResults(
            self.expr, start, end, [], ["add exceptions for missed whitespace"]
        )

    def skip(self, string, start):
        """
        :return: next non-whitespace position
        """
        if not self.ignore_list and not self.white_chars:
            return start
        if string is self.content:
            try:
                end = self.skips[start]
                if end != -1:
                    return end
            except IndexError:
                return start
        else:
            num = len(string)
            self.skips = [-1] * num
            self.content = string
            if start >= num:
                return start

        end = start  # TO AVOID RECURSIVE LOOP
        found = self.regex.match(string, start)
        if found:
            end = found.end()
        self.skips[start] = end  # THE REAL VALUE
        return end

    def __regex__(self):
        if self._in_regex:
            return "*", ""
        self._in_regex = True
        try:
            white = regex_range(self.white_chars)
            if not self.ignore_list:
                if not white:
                    return "*", ""
                else:
                    return "*", white + "*"

            ignored = "|".join(regex_iso(*i.__regex__(), "|") for i in self.ignore_list)
            return "+", f"(?:{white}*(?:{ignored}))*{white}*"
        finally:
            self._in_regex = False

    def __str__(self):
        output = ["{"]
        for k, v in self.__dict__.items():
            value = str(v)
            output.append(indent(quote(k) + ":" + value))
        output.append("}")
        return "\n".join(output)


class Backup(object):
    def __init__(self, whitespace):
        self.whitespace = whitespace
        self.content = whitespace.content
        self.skips = whitespace.skips

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.whitespace.content = self.content
        self.whitespace.skips = self.skips


def noop(*args):
    return


DebugActions = namedtuple("DebugActions", ["TRY", "MATCH", "FAIL"])
