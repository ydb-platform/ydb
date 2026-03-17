# encoding: utf-8
import sys
from collections import namedtuple
from threading import RLock
from typing import List

from mo_future import text, first
from mo_imports import export, expect

from mo_parsing import whitespaces
from mo_parsing.exceptions import ParseException
from mo_parsing.results import ParseResults
from mo_parsing.utils import Log, MAX_INT, wrap_parse_action, empty_tuple

(
    SkipTo,
    Many,
    ZeroOrMore,
    OneOrMore,
    Optional,
    NotAny,
    Suppress,
    And,
    MatchFirst,
    Or,
    MatchAll,
    Empty,
    StringEnd,
    Literal,
    Token,
    Group,
    regex_parameters,
    _suppress_post_parse
) = expect(
    "SkipTo",
    "Many",
    "ZeroOrMore",
    "OneOrMore",
    "Optional",
    "NotAny",
    "Suppress",
    "And",
    "MatchFirst",
    "Or",
    "MatchAll",
    "Empty",
    "StringEnd",
    "Literal",
    "Token",
    "Group",
    "regex_parameters",
    "_suppress_post_parse"
)

DEBUG = False

# TODO: Replace with a stack of parse state
_reset_actions = []


def add_reset_action(action):
    """
    ADD A FUNCTION THAT WILL RESET GLOBAL STATE THAT A PARSER MAY USE
    :param action:  CALLABLE
    """
    _reset_actions.append(action)


locker = RLock()
streamlined = {}


def entrypoint(func):
    def output(*args, **kwargs):
        with locker:
            for a in _reset_actions:
                try:
                    a()
                except Exception as e:
                    Log.error("reset action failed", cause=e)

            return func(*args, **kwargs)

    return output


def _verify_whitespace(whi: List):
    if whi is None:
        return None
    if isinstance(whi, list):
        whis = [
            v
            for e in whi
            for v in [_verify_whitespace(e)]
            if v is not None and v.regex.pattern  # IGNORE NO_WHITESPACE
        ]
        if not whis:
            return None
        whitespace = whis[0]
        if any(e.id != whitespace.id for e in whis[1:]):
            # THE TOP-MOST WHITESPACE RULES ARE DIFFERENT FOR EACH ParserElement,
            # SO PROGRAM DOES NOT KNOW WHICH IS THE MASTER WHITESPACE
            Log.error("must dis-ambiguate the whitespace before parsing")
        return whitespace
    return whi


class Parser(object):
    def __init__(self, element):
        self.element = element = element.streamline()
        try:
            self.whitespace = (
                _verify_whitespace(element.whitespace)
                or whitespaces.CURRENT
                or whitespaces.STANDARD_WHITESPACE
            )
        except Exception as cause:
            Log.error("problem", cause=cause)

        with self.whitespace:
            self.element = Group(element)

        self.named = bool(element.token_name)
        self.streamlined = True

    @entrypoint
    def parse(self, string, parse_all=False):
        """
        Parse a string with respect to the parser definition. This function is intended as the primary interface to the
        client code.

        :param string: The input string to be parsed.
        :param parse_all: If set, the entire input string must match the grammar.
        :raises ParseException: Raised if ``parse_all`` is set and the input string does not match the whole grammar.
        :returns: the parsed data as a `ParseResults` object, which may be accessed as a `list`, a `dict`, or
          an object with attributes if the given parser includes results names.

        If the input string is required to match the entire grammar, ``parse_all`` flag must be set to True. This
        is also equivalent to ending the grammar with ``StringEnd()``.

        To report proper column numbers, ``parse_string`` operates on a copy of the input string where all tabs are
        converted to spaces (8 spaces per tab, as per the default in ``string.expandtabs``). If the input string
        contains tabs and the grammar uses parse actions that use the ``loc`` argument to index into the string
        being parsed, one can ensure a consistent view of the input string by doing one of the following:

        - define your parse action using the full ``(s,loc,toks)`` signature, and reference the input string using the
          parse action's ``s`` argument, or
        - explicitly expand the tabs in your input string before calling ``parse_string``.

        """
        return self._parseString(string, parse_all=parse_all)

    parse_string = parse

    def _parseString(self, string, parse_all=False):
        start = self.whitespace.skip(string, 0)
        try:
            tokens = self.element._parse(string, start)
            if parse_all:
                end = self.whitespace.skip(string, tokens.end)
                try:
                    StringEnd()._parse(string, end)
                except ParseException as pe:
                    raise ParseException(
                        self.element, 0, string, cause=tokens.failures + [pe]
                    ) from None

            if self.named:
                return tokens
            else:
                return tokens.tokens[0]
        except ParseException as cause:
            raise cause.best_cause from None

    @entrypoint
    def scan_string(self, string, max_matches=MAX_INT, overlap=False):
        """
        :param string: TO BE SCANNED
        :param max_matches: MAXIMUM NUMBER MATCHES TO RETURN
        :param overlap: IF MATCHES CAN OVERLAP
        :return: SEQUENCE OF ParseResults, start, end
        """
        return (
            (t.tokens[0], s, e)
            for t, s, e in self._scan_string(
                string, max_matches=max_matches, overlap=overlap
            )
        )

    def _scan_string(self, string, max_matches=MAX_INT, overlap=False):
        instrlen = len(string)
        start = end = 0
        matches = 0
        while end <= instrlen and matches < max_matches:
            try:
                start = self.whitespace.skip(string, end)
                tokens = self.element._parse(string, start)
            except ParseException:
                end = start + 1
            else:
                matches += 1
                yield tokens, tokens.start, tokens.end
                if overlap or tokens.end <= end:
                    end += 1
                else:
                    end = tokens.end

    @entrypoint
    def transform_string(self, string):
        """
        Modify matching text with results of a parse action.

        To use ``transform_string``, define a grammar and
        attach a parse action to it that modifies the returned token list.
        Invoking ``transform_string()`` on a target string will then scan for matches,
        and replace the matched text patterns according to the logic in the parse
        action.  ``transform_string()`` returns the resulting transformed string.

        Example::

            wd = Word(alphas)
            wd.add_parse_action(lambda toks: toks[0].title())

            print(wd.transform_string("now is the winter of our discontent made glorious summer by this sun of york."))

        prints::

            Now Is The Winter Of Our Discontent Made Glorious Summer By This Sun Of York.
        """
        return self._transformString(string)

    def _transformString(self, string):
        out = []
        end = 0
        # force preservation of <TAB>s, to minimize unwanted transformation of string, and to
        # keep string locs straight between transform_string and scan_string
        for t, s, e in self._scan_string(string):
            out.append(string[end:s])
            t = t.tokens[0]
            if t:
                if isinstance(t, ParseResults):
                    out.append("".join(t))
                elif isinstance(t, list):
                    out.append("".join(t))
                else:
                    out.append(t)
            end = e
        out.append(string[end:])
        out = [o for o in out if o]
        return "".join(map(text, out))

    @entrypoint
    def search_string(self, string, max_matches=MAX_INT):
        """
        :param string: Content to scan
        :param max_matches: Limit number of matches
        :return: All the matches, packaged as ParseResults
        """
        return self._search_string(string, max_matches=max_matches)

    def _search_string(self, string, max_matches=MAX_INT):
        scanned = [t for t, s, e in self._scan_string(string, max_matches)]
        if not scanned:
            return ParseResults(ZeroOrMore(self.element), -1, 0, [], [])
        else:
            return ParseResults(
                ZeroOrMore(self.element),
                scanned[0].start,
                scanned[-1].end,
                scanned,
                scanned[-1].failures,
            )

    @entrypoint
    def split(self, string, maxsplit=MAX_INT, include_separators=False):
        """
        Generator method to split a string using the given expression as a separator.
        May be called with optional ``maxsplit`` argument, to limit the number of splits;
        and the optional ``include_separators`` argument (default= ``False``), if the separating
        matching text should be included in the split results.

        Example::

            punc = one_of(list(".,;:/-!?"))
            print(list(punc.split("This, this?, this sentence, is badly punctuated!")))

        prints::

            ['This', ' this', '', ' this sentence', ' is badly punctuated', '']
        """
        return self._split(
            string, maxsplit=maxsplit, include_separators=include_separators
        )

    def _split(self, string, maxsplit=MAX_INT, include_separators=False):
        last = 0
        for t, s, e in self._scan_string(string, max_matches=maxsplit):
            yield string[last:s]
            if include_separators:
                yield t.tokens[0]
            last = e
        yield string[last:]


class ParserElement(object):
    """Abstract base level parser element class."""

    zero_length = False
    __slots__ = [
        "parse_action",
        "parser_name",
        "token_name",
        "streamlined",
        "min_length_cache",
        "parser_config",
    ]
    Config = namedtuple("Config", ["callDuringTry", "fail_action"])

    def __init__(self):
        self.parse_action = list()
        self.parser_name = ""
        self.token_name = ""
        self.streamlined = False
        self.min_length_cache = -1

        self.parser_config = self.Config(*([None] * len(self.Config._fields)))
        self.set_config(callDuringTry=False, fail_action=None)

    def set_config(self, **map):
        data = {
            **dict(zip(self.parser_config.__class__._fields, self.parser_config)),
            **map,
        }
        self.parser_config = self.Config(*(data[f] for f in self.Config._fields))

    def copy(self):
        output = object.__new__(self.__class__)
        output.parse_action = self.parse_action[:]
        output.parser_name = self.parser_name
        output.token_name = self.token_name
        output.parser_config = self.parser_config
        output.streamlined = self.streamlined
        output.min_length_cache = -1
        return output

    def set_parser_name(self, name):
        """
        Define name for this expression, makes debugging and exception messages clearer.

        Example::

            Word(nums).parse_string("ABC")  # -> Exception: Expected W:(0123...) (at char 0), (line:1, col:1)
            Word(nums).set_parser_name("integer").parse_string("ABC")  # -> Exception: Expected integer (at char 0), (line:1, col:1)
        """
        self.parser_name = name
        return self

    def clear_parse_action(self):
        """
        Add one or more parse actions to expression's list of parse actions. See `setParseAction`.

        See examples in `copy`.
        """
        output = self.copy()
        output.parse_action = []
        return output

    def add_parse_action(self, *fns, callDuringTry=False):
        """
        Add one or more parse actions to expression's list of parse actions. See `setParseAction`.
        Also you can use `/` operator
        """
        output = self.copy()
        output.parse_action += list(map(wrap_parse_action, fns))
        output.set_config(
            callDuringTry=self.parser_config.callDuringTry or callDuringTry
        )
        return output

    def __truediv__(self, func):
        """
        Shortform for add_parse_action
        """
        output = self.copy()
        try:
            output.parse_action.append(wrap_parse_action(func))
        except:
            # REPLACE WITH CONSTANT
            output.parse_action.append(lambda t, i, s: ParseResults(t.type, t.start, t.end, [func], []))
        return output

    def add_condition(self, *fns, message=None, callDuringTry=False, fatal=False):
        """
        Add a boolean predicate function to expression's list of parse actions. See
        `setParseAction` for function call signatures. Unlike ``setParseAction``,
        functions passed to ``add_condition`` need to return boolean success/fail of the condition.

        Optional keyword arguments:
        - message = define a custom message to be used in the raised exception
        - fatal   = if True, will raise ParseFatalException to stop parsing immediately; otherwise will raise ParseException

        """

        def make_cond(fn):
            def cond(token, index, string):
                result = fn(token, index, string)
                if not bool(result.tokens[0]):
                    error = ParseException(token.type, index, string, msg=message)
                    if fatal:
                        Log.error("fatal error", cause=error)
                    raise error
                return token

            return cond

        output = self.copy()
        for fn in fns:
            output.parse_action.append(make_cond(wrap_parse_action(fn)))

        output.set_config(
            callDuringTry=self.parser_config.callDuringTry or callDuringTry
        )
        return output

    def setFailAction(self, fn):
        """Define action to perform if parsing fails at this expression.
        Fail acton fn is a callable function that takes the arguments
        ``fn(s, loc, expr, err)`` where:
        - expr = the parse expression that failed
        - loc = location where expression match was attempted and failed
        - s = string being parsed
        - err = the exception thrown
        The function returns no value.  It may throw `ParseFatalException`
        if it is desired to stop parsing immediately."""
        self.set_config(fail_action=fn)
        return self

    def is_annotated(self):
        action = first(a for a in self.parse_action if a is not _suppress_post_parse)
        return action or self.token_name or self.parser_name

    def expecting(self):
        """
        RETURN EXPECTED CHARACTER SEQUENCE, IF ANY
        :return:
        """
        return {}

    def min_length(self):
        if self.min_length_cache >= 0:
            return self.min_length_cache
        min_ = self._min_length()
        if self.streamlined:
            self.min_length_cache = min_
        return min_

    def _min_length(self):
        return 0

    @property
    def whitespace(self):
        return None

    def parse_impl(self, string, start, do_actions=True):
        return ParseResults(self, start, start, [], [])

    def _parse(self, string, start, do_actions=True):
        try:
            result = self.parse_impl(string, start, do_actions)
        except ParseException as cause:
            self.parser_config.fail_action and self.parser_config.fail_action(
                self, start, string, cause
            )
            raise ParseException(self, start, string, cause=cause) from None

        if do_actions or self.parser_config.callDuringTry:
            for fn in self.parse_action:
                next_result = fn(result, result.start, string)
                if next_result.end < result.end:
                    Log.error(
                        "parse action {{name}} not allowed to roll back the end of parsing",
                        name=fn.__name__
                    )
                result = next_result
        return result

    def finalize(self):
        """
        Return a Parser for use in parsing (optimization only)
        :return:
        """
        return Parser(self)

    def parse(self, string, parse_all=False):
        return self.finalize().parse(string, parse_all)

    parse_string = parse

    def scan_string(self, string, max_matches=MAX_INT, overlap=False):
        return (
            self
            .finalize()
            .scan_string(string, max_matches=max_matches, overlap=overlap)
        )

    def transform_string(self, string):
        return self.finalize().transform_string(string)

    def search_string(self, string, max_matches=MAX_INT):
        return self.finalize().search_string(string, max_matches=max_matches)

    def split(self, string, maxsplit=MAX_INT, include_separators=False):
        return (
            self
            .finalize()
            .split(string, maxsplit=maxsplit, include_separators=include_separators)
        )

    def replace_with(self, replacement):
        """
        Add parse action that replaces the token with replacement

        RegEx variables are accepted:
        \\1
        \\g<1>
        \\g<name>
        """

        # FIND NAMES IN replacement
        parts = list(regex_parameters.split(replacement, include_separators=True))

        def replacer(tokens):
            acc = []
            for s, n in zip(parts, parts[1:]):
                acc.append(s)
                acc.append(text(tokens[n]))
            acc.append(parts[-1])
            return "".join(acc)

        return self / replacer

    sub = replace_with

    def __add__(self, other):
        """
        Implementation of + operator - returns `And`. Adding strings to a ParserElement
        converts them to `Literal`s by default.
        """
        if other is Ellipsis:
            return _PendingSkip(self)

        return And(
            [self, whitespaces.CURRENT.normalize(other)], whitespaces.CURRENT
        ).streamline()

    def __radd__(self, other):
        """
        Implementation of + operator when left operand is not a `ParserElement`
        """
        if other is Ellipsis:
            return SkipTo(self)("_skipped") + self

        return whitespaces.CURRENT.normalize(other) + self

    def __sub__(self, other):
        """
        Implementation of - operator, returns `And` with error stop
        """
        return self + And.SyntaxErrorGuard() + whitespaces.CURRENT.normalize(other)

    def __rsub__(self, other):
        """
        Implementation of - operator when left operand is not a `ParserElement`
        """
        return whitespaces.CURRENT.normalize(other) - self

    def __mul__(self, other):
        """
        Implementation of * operator, allows use of ``expr * 3`` in place of
        ``expr + expr + expr``.  Expressions may also me multiplied by a 2-integer
        tuple, similar to ``{min, max}`` multipliers in regular expressions.  Tuples
        may also include ``None`` as in:
         - ``expr*(n, None)`` or ``expr*(n, )`` is equivalent
              to ``expr*n + ZeroOrMore(expr)``
              (read as "at least n instances of ``expr``")
         - ``expr*(None, n)`` is equivalent to ``expr*(0, n)``
              (read as "0 to n instances of ``expr``")
         - ``expr*(None, None)`` is equivalent to ``ZeroOrMore(expr)``
         - ``expr*(1, None)`` is equivalent to ``OneOrMore(expr)``

        Note that ``expr*(None, n)`` does not raise an exception if
        more than n exprs exist in the input stream; that is,
        ``expr*(None, n)`` does not enforce a maximum number of expr
        occurrences.  If this behavior is desired, then write
        ``expr*(None, n) + ~expr``
        """
        if isinstance(other, tuple):
            min_elements, max_elements = (other + (None, None))[:2]
        else:
            min_elements, max_elements = other, other

        if min_elements == Ellipsis or not min_elements:
            min_elements = 0
        elif not isinstance(min_elements, int):
            raise TypeError(
                "cannot multiply 'ParserElement' and ('%s', '%s') objects",
                type(other[0]),
                type(other[1]),
            )
        elif min_elements < 0:
            raise ValueError("cannot multiply ParserElement by negative value")

        if max_elements == Ellipsis or not max_elements:
            max_elements = MAX_INT
        elif (
            not isinstance(max_elements, int)
            or max_elements < min_elements
            or max_elements == 0
        ):
            raise TypeError(
                "cannot multiply 'ParserElement' and ('%s', '%s') objects",
                type(other[0]),
                type(other[1]),
            )

        ret = Many(
            self, whitespaces.CURRENT, min_match=min_elements, max_match=max_elements
        ).streamline()
        return ret

    def __rmul__(self, other):
        return self.__mul__(other)

    def __or__(self, other):
        """
        Implementation of | operator - returns `MatchFirst`
        """
        if other is Ellipsis:
            return _PendingSkip(Optional(self))

        return MatchFirst([self, whitespaces.CURRENT.normalize(other)]).streamline()

    def __ror__(self, other):
        """
        Implementation of | operator when left operand is not a `ParserElement`
        """
        return whitespaces.CURRENT.normalize(other) | self

    def __xor__(self, other):
        """
        Implementation of ^ operator - returns `Or`
        """
        return Or([self, whitespaces.CURRENT.normalize(other)])

    def __rxor__(self, other):
        """
        Implementation of ^ operator when left operand is not a `ParserElement`
        """
        return whitespaces.CURRENT.normalize(other) ^ self

    def __and__(self, other):
        """
        Implementation of & operator - returns `MatchAll`
        """
        return MatchAll([self, whitespaces.CURRENT.normalize(other)])

    def __rand__(self, other):
        """
        Implementation of & operator when left operand is not a `ParserElement`
        """
        return whitespaces.CURRENT.normalize(other) & self

    def __invert__(self):
        """
        Implementation of ~ operator - returns `NotAny`
        """
        return NotAny(self)

    def __getitem__(self, key):
        if isinstance(key, slice):
            return self * (key.start, key.stop)
        return self * key

    def __call__(self, name):
        """
        Shortcut for `.set_token_name`, with ``listAllMatches=False``.
        """
        if not name:
            return self
        return self.set_token_name(name)

    def reverse(self):
        raise NotImplementedError()

    def set_token_name(self, name):
        """
        SET name AS PART OF A LARGER GROUP
        :param name:
        """
        output = self.copy()
        output.token_name = name
        return output

    def suppress(self):
        """
        Suppresses the output of this `ParserElement`; useful to keep punctuation from
        cluttering up returned output.
        """
        return self / _suppress_post_parse

    def __str__(self):
        return self.parser_name

    def __repr__(self):
        return text(self)

    def streamline(self):
        self.streamlined = True
        return self

    def check_recursion(self, seen=empty_tuple):
        pass

    def parse_file(self, file_or_filename, parse_all=False):
        """
        Execute the parse expression on the given file or filename.
        If a filename is specified (instead of a file object),
        the entire file is opened, read, and closed before parsing.
        """
        try:
            file_contents = file_or_filename.read()
        except AttributeError:
            with open(file_or_filename, "r") as f:
                file_contents = f.read()
        return self.parse_string(file_contents, parse_all)

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return id(self)

    def __req__(self, other):
        return self == other

    def __rne__(self, other):
        return not (self == other)

    def matches(self, test_string, parse_all=True):
        """
        Method for quick testing of a parser against a test string. Good for simple
        inline microtests of sub expressions while building up larger parser.

        Parameters:
         - test_string - to test against this expression for a match
         - parse_all - (default= ``True``) - flag to pass to `parse_string` when running tests

        Example::

            expr = Word(nums)
            assert expr.matches("100")
        """
        try:
            self.parse_string(text(test_string), parse_all=parse_all)
            return True
        except ParseException:
            return False


class _PendingSkip(ParserElement):
    # internal placeholder class to hold a place were '...' is added to a parser element,
    # once another ParserElement is added, this placeholder will be replaced with a SkipTo
    def __init__(self, expr):
        super(_PendingSkip, self).__init__()
        self.anchor = expr
        self.parser_name = "pending_skip"

    def __add__(self, other):
        if isinstance(other, _PendingSkip):
            return self.anchor + other

        skipper = SkipTo(other)("_skipped")
        return self.anchor + skipper + other

    def parse_impl(self, *args):
        Log.error("use of `...` expression without following SkipTo target expression")


def set_parser_names():
    """
    ASSIGN parser_name FOR All ParserElements FOUND IN CALLER
    """
    frame = sys._getframe(1)
    items = list(frame.f_locals.items())
    for k, v in items:
        try:
            if isinstance(v, ParserElement) and not v.parser_name:
                v.parser_name = k.lower()
        except Exception:
            pass


NO_PARSER = (
    ParserElement().set_parser_name("<nothing>")
)  # USE THIS WHEN YOU DO NOT CARE ABOUT THE PARSER TYPE
NO_RESULTS = ParseResults(NO_PARSER, -1, 0, [], [])

export("mo_parsing.results", ParserElement)
export("mo_parsing.results", NO_PARSER)
export("mo_parsing.results", NO_RESULTS)
