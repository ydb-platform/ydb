# encoding: utf-8
from collections import OrderedDict

from mo_dots import Null, is_null
from mo_future import text, is_text
from mo_imports import export, expect

from mo_parsing import whitespaces
from mo_parsing.core import ParserElement
from mo_parsing.exceptions import ParseException, RecursiveGrammarException
from mo_parsing.results import ParseResults, ForwardResults, Annotation
from mo_parsing.utils import (
    Log,
    enlist,
    empty_tuple,
    regex_iso,
    append_config,
    regex_compile,
    wrap_parse_action,
)
from mo_parsing.utils import MAX_INT, is_forward

Word: object
(Token, NoMatch, Literal, Keyword, Word, CharsNotIn, StringEnd, Empty, Char,) = expect(
    "Token", "NoMatch", "Literal", "Keyword", "Word", "CharsNotIn", "StringEnd", "Empty", "Char",
)

_get = object.__getattribute__


class ParseEnhancement(ParserElement):
    """
    Abstract subclass of `ParserElement`, for combining and
    post-processing parsed tokens.
    """

    __slots__ = ["expr"]

    def __init__(self, expr):
        ParserElement.__init__(self)
        self.expr = expr = whitespaces.CURRENT.normalize(expr)
        if is_forward(expr):
            expr.track(self)

    def copy(self):
        output = ParserElement.copy(self)
        output.expr = self.expr
        return output

    def expecting(self):
        if self.expr:
            return OrderedDict(((k, [self]) for k, e in self.expr.expecting().items()))
        else:
            return {}

    def _min_length(self):
        return self.expr.min_length()

    @property
    def whitespace(self):
        return self.expr.whitespace

    def parse_impl(self, string, start, do_actions=True):
        try:
            result = self.expr._parse(string, start, do_actions)
            return ParseResults(self, result.start, result.end, [result], result.failures)
        except ParseException as cause:
            raise ParseException(self, start, string, cause=cause) from None

    def streamline(self):
        if self.streamlined:
            return self

        expr = self.expr.streamline()
        if expr is self.expr:
            self.streamlined = True
            return self

        if not expr or isinstance(expr, Empty) and not self.is_annotated():
            return Empty()

        output = self.copy()
        output.expr = expr
        output.streamlined = True
        return output

    def check_recursion(self, seen=empty_tuple):
        if self in seen:
            raise RecursiveGrammarException(seen + (self,))
        if self.expr != None:
            self.expr.check_recursion(seen + (self,))

    def __str__(self):
        if self.parser_name:
            return self.parser_name
        return f"{self.__class__.__name__}:({self.expr})"


class LookAhead(ParseEnhancement):
    """
    Lookahead matching of the given parse expression.
    `LookAhead` does *not* advance the parsing position within
    the input string, it only verifies that the specified parse
    expression matches at the current position.  `LookAhead`
    always returns a null token list. If any results names are defined
    in the lookahead expression, those *will* be returned for access by
    name.
    """

    zero_length = True
    __slots__ = []

    def __init__(self, expr):
        ParseEnhancement.__init__(self, expr)

    def parse_impl(self, string, start, do_actions=True):
        # by using self._expr.parse and deleting the contents of the returned ParseResults list
        # we keep any named results that were defined in the FollowedBy expression
        result = self.expr._parse(string, start, do_actions=do_actions)
        result.__class__ = Annotation

        return ParseResults(self, start, start, [result], result.failures)

    def __regex__(self):
        return "*", f"(?={self.expr.__regex__()[1]})"


FollowedBy = LookAhead


class NotAny(LookAhead):
    """
    Lookahead to disallow matching with the given parse expression.
    May be constructed using the '~' operator.
    """

    zero_length = True
    __slots__ = ["regex"]

    def __init__(self, expr):
        super(NotAny, self).__init__(expr)
        try:
            prec, pattern = self.expr.__regex__()
            self.regex = regex_compile(f"(?!{pattern})")
        except Exception as c:
            self.regex = None

    def copy(self):
        output = ParseEnhancement.copy(self)
        output.regex = self.regex
        return output

    def parse_impl(self, string, start, do_actions=True):
        regex = self.regex
        if regex:
            found = regex.match(string, start)
            if found:
                return ParseResults(self, start, start, [], [])
            raise ParseException(self, start, string) from None
        else:
            try:
                self.expr.parse(string, start, do_actions=True)
                raise ParseException(self, start, string) from None
            except:
                return ParseResults(self, start, start, [], [])

    def streamline(self):
        output = ParseEnhancement.streamline(self)
        if isinstance(output, Empty):
            return NoMatch()
        if isinstance(output.expr, NoMatch):
            return Empty()
        if isinstance(output.expr, Empty):
            return NoMatch()
        return output

    def expecting(self):
        return {}

    def min_length(self):
        return 0

    def __regex__(self):
        return "*", self.regex.pattern

    def __str__(self):
        if self.parser_name:
            return self.parser_name
        return "~{" + str(self.expr) + "}"


class Many(ParseEnhancement):
    __slots__ = []
    Config = append_config(ParseEnhancement, "whitespace", "min_match", "max_match", "end")

    def __init__(
        self, expr, whitespace=None, stop_on=None, min_match=0, max_match=MAX_INT, exact=None,
    ):
        """
        MATCH expr SOME NUMBER OF TIMES (OR UNTIL stop_on IS REACHED
        :param expr: THE EXPRESSION TO MATCH
        :param stop_on: THE PATTERN TO INDICATE STOP MATCHING (NOT REQUIRED IN PATTERN, JUST A QUICK STOP)
        :param min_match: MINIMUM MATCHES REQUIRED FOR SUCCESS (-1 IS INVALID)
        :param max_match: MAXIMUM MATCH REQUIRED FOR SUCCESS (-1 IS INVALID)
        """
        ParseEnhancement.__init__(self, expr)
        if isinstance(self.expr, LookBehind):
            # TODO: support Optional(LookBehind()))
            Log.error("can only look behind once")
        if exact is not None:
            min_match = exact
            max_match = exact

        self.set_config(
            whitespace=whitespace or whitespaces.CURRENT, min_match=min_match, max_match=max_match,
        )
        self.stop_on(stop_on)

    def stop_on(self, ender):
        if ender:
            end = self.parser_config.whitespace.normalize(ender)
            self.set_config(end=regex_compile(end.__regex__()[1]))
        return self

    def _min_length(self):
        if self.parser_config.min_match == 0:
            return 0
        return self.expr.min_length()

    @property
    def whitespace(self):
        return self.parser_config.whitespace

    def parse_impl(self, string, start, do_actions=True):
        acc = []
        end = start
        max = self.parser_config.max_match
        min = self.parser_config.min_match
        stopper = self.parser_config.end
        count = 0
        failures = []
        try:
            while end < len(string):
                index = self.parser_config.whitespace.skip(string, end)
                if stopper:
                    if stopper.match(string, index):
                        if min <= count:
                            break
                        else:
                            raise ParseException(self, end, string, msg="found stopper too soon")
                result = self.expr._parse(string, index, do_actions)
                end = result.end
                if result.end - result.start:
                    acc.append(result)
                    failures.extend(result.failures)
                    count += 1
                    if count >= max:
                        break

        except ParseException as cause:
            failures.append(cause)

        if count < min:
            raise ParseException(self, start, string, f"Expecting at least {min} of {self}", failures)
        elif max < count:
            raise ParseException(
                self, acc[0].start, string, f"Expecting less than {max} of {self.expr}", failures,
            )
        else:
            if count:
                return ParseResults(self, acc[0].start, acc[-1].end, acc, failures)
            else:
                return ParseResults(self, start, end, acc, failures)

    def streamline(self):
        if self.streamlined:
            return self
        expr = self.expr.streamline()
        if self.parser_config.min_match == self.parser_config.max_match and not self.is_annotated():
            if self.parser_config.min_match == 0:
                return Empty()
            elif self.parser_config.min_match == 1:
                return expr

        if self.expr is expr:
            self.streamlined = True
            return self
        if expr.is_annotated() or not isinstance(expr, Empty):
            output = self.copy()
            output.expr = expr
            output.streamlined = True
            return output
        return Empty()

    def __regex__(self):
        end = self.parser_config.end.pattern if self.parser_config.end else None
        prec, regex = self.expr.__regex__()
        regex = regex_iso(prec, regex, "*")

        if self.parser_config.max_match == MAX_INT:
            if self.parser_config.min_match == 0:
                suffix = "*"
            elif self.parser_config.min_match == 1:
                suffix = "+"
            else:
                suffix = "{" + text(self.parser_config.min_match) + ",}"
        elif self.parser_config.min_match == self.parser_config.max_match:
            if self.parser_config.min_match == 1:
                suffix = ""
            else:
                suffix = "{" + text(self.parser_config.min_match) + "}"
        else:
            suffix = "{" + text(self.parser_config.min_match) + "," + text(self.parser_config.max_match) + "}"

        if end:
            return "+", regex + suffix + end
        else:
            return "*", regex + suffix

    def __call__(self, name):
        if not name:
            return self

        for e in [self.expr]:
            if isinstance(e, ParserElement) and e.token_name == name:
                Log.error("can not set token name, already set in one of the other expressions")

        return ParseEnhancement.__call__(self, name)

    def __str__(self):
        if self.parser_name:
            return self.parser_name
        return f"{self.__class__.__name__}:({self.expr})"


class OneOrMore(Many):
    """Repetition of one or more of the given expression.

    Parameters:
     - expr - expression that must match one or more times
     - stop_on - (default= ``None``) - expression for a terminating sentinel
          (only required if the sentinel would ordinarily match the repetition
          expression)
    """

    __slots__ = []

    def __init__(self, expr, whitespace=None, stop_on=None):
        Many.__init__(self, expr, whitespace, stop_on, min_match=1, max_match=MAX_INT)

    def __str__(self):
        if self.parser_name:
            return self.parser_name
        return "{" + text(self.expr) + "}+"


class ZeroOrMore(Many):
    """Optional repetition of zero or more of the given expression.

    Parameters:
     - expr - expression that must match zero or more times
     - stop_on - (default= ``None``) - expression for a terminating sentinel
          (only required if the sentinel would ordinarily match the repetition
          expression)

    Example: similar to `OneOrMore`
    """

    __slots__ = []

    def __init__(self, expr, whitespace=None, stop_on=None):
        Many.__init__(self, expr, whitespace, stop_on=stop_on, min_match=0, max_match=MAX_INT)

    def parse_impl(self, string, start, do_actions=True):
        try:
            return Many.parse_impl(self, string, start, do_actions)
        except ParseException as pe:
            return ParseResults(self, start, start, [], [pe])

    def __str__(self):
        if self.parser_name:
            return self.parser_name

        return "(" + text(self.expr) + ")*"


class Optional(Many):
    """Optional matching of the given expression.

    Parameters:
     - expr - expression that must match zero or more times
     - default (optional) - value to be returned if the optional expression is not found.
    """

    __slots__ = []
    Config = append_config(Many, "default_value")

    def __init__(self, expr, whitespace=None, default=None):
        Many.__init__(self, expr, whitespace, stop_on=None, min_match=0, max_match=1)
        self.set_config(default_value=enlist(default))

    def parse_impl(self, string, start, do_actions=True):
        try:
            results = self.expr._parse(string, start, do_actions)
            return ParseResults(self, results.start, results.end, [results], results.failures)
        except ParseException as pe:
            return ParseResults(self, start, start, self.parser_config.default_value, [pe])

    def __str__(self):
        if self.parser_name:
            return self.parser_name

        return "[" + text(self.expr) + "]"


class SkipTo(ParseEnhancement):
    """Token for skipping over all undefined text until the matched expression is found."""

    __slots__ = []
    Config = append_config(ParseEnhancement, "include", "fail", "ignore", "whitespace")

    def __init__(self, expr, include=False, ignore=None, fail_on=None, engine_=None):
        """
        :param expr: target expression marking the end of the data to be skipped
        :param include: if True, the target expression is also parsed
          (the skipped text and target expression are returned as a 2-element list).
        :param ignore: used to define grammars (typically quoted strings and
          comments) that might contain false matches to the target expression
        :param fail_on: define expressions that are not allowed to be
          included in the skipped test; if found before the target expression is found,
          the SkipTo is not a match
        """
        ParseEnhancement.__init__(self, expr)

        self.set_config(
            include=include,
            fail=whitespaces.CURRENT.normalize(fail_on),
            ignore=ignore,
            whitespace=engine_ or whitespaces.CURRENT,
        )
        self.parser_name = str(self)

    def min_length(self):
        return 0

    def __regex__(self):
        prec, pattern = self.expr.__regex__()
        pattern = regex_iso(prec, pattern, "+")
        if self.parser_config.include:
            return "*", f"(.*?{pattern})"
        else:
            return "+", f"(.*?)(?={pattern})"

    def parse_impl(self, string, start, do_actions=True):
        instrlen = len(string)
        fail = self.parser_config.fail
        ignore = self.parser_config.ignore

        loc = start
        while loc <= instrlen:
            skip_end = loc
            loc = before_end = self.parser_config.whitespace.skip(string, loc)
            if fail:
                # break if fail_on expression matches
                try:
                    fail._parse(string, loc)
                    break
                except:
                    pass

            if ignore:
                # advance past ignore expressions
                while 1:
                    try:
                        loc = ignore._parse(string, loc).end
                        skip_end = loc
                        loc = before_end = self.parser_config.whitespace.skip(string, loc)
                    except ParseException:
                        break
            try:
                loc = self.expr._parse(string, loc, do_actions=False).end
            except ParseException:
                # no match, advance loc in string
                loc += 1
            else:
                # matched skipto expr, done
                break
        else:
            # ran off the end of the input string without matching skipto expr, fail
            raise ParseException(self, start, string) from None

        # build up return values
        end = loc
        skiptext = string[start:skip_end]
        skip_result = []
        if skiptext:
            skip_result.append(skiptext)

        if self.parser_config.include:
            end_result = self.expr._parse(string, before_end, do_actions)
            skip_result.append(end_result)
            return ParseResults(self, start, end, skip_result, [])
        else:
            return ParseResults(self, start, before_end, skip_result, [])


class Forward(ParserElement):
    """Forward declaration of an expression to be defined later -
    used for recursive grammars, such as algebraic infix notation.
    When the expression is known, it is assigned to the ``Forward``
    variable using the '<<' operator.

    Note: take care when assigning to ``Forward`` not to overlook
    precedence of operators.

    Specifically, '|' has a lower precedence than '<<', so that::

        fwd_expr << a | b | c

    will actually be evaluated as::

        (fwd_expr << a) | b | c

    thereby leaving b and c out as parseable alternatives.  It is recommended that you
    explicitly group the values inserted into the ``Forward``::

        fwd_expr << (a | b | c)

    Converting to use the '<<=' operator instead will avoid this problem.

    See `ParseResults.pprint` for an example of a recursive
    parser created using ``Forward``.
    """

    __slots__ = [
        "expr",
        "used_by",
        "_str",
        "_in_regex",
        "_in_expecting",
        "__in_whitespace",
    ]

    def __init__(self, expr=Null):
        ParserElement.__init__(self)
        self.expr = None
        self.used_by = []

        self._str = None  # avoid recursion
        self._in_regex = None  # avoid recursion
        self._in_expecting = None  # avoid recursion
        self.__in_whitespace = False
        if expr:
            self << whitespaces.CURRENT.normalize(expr)

    def copy(self):
        output = ParserElement.copy(self)
        output.expr = self
        output._str = None
        output._in_regex = None
        output._in_expecting = None
        output.__in_whitespace = False

        output.used_by = []
        return output

    @property
    def name(self):
        return self.type.expr.token_name

    def track(self, expr):
        self.used_by.append(expr)

    def __lshift__(self, other):
        self._str = ""
        if is_forward(self.expr):
            return self.expr << other

        while is_forward(other):
            other = other.expr
        norm = whitespaces.CURRENT.normalize(other)
        self.expr = norm.streamline()
        self.check_recursion()
        return self

    def add_parse_action(self, action):
        """
        Add one or more parse actions to expression's list of parse actions. See `setParseAction`.
        Also you can use `/` operator
        """
        self.parse_action.append(wrap_parse_action(action))
        return self

    def leave_whitespace(self):
        with whitespaces.NO_WHITESPACE:
            output = self.copy()
            output.expr = self.expr.leave_whitespace()
            return output

    def streamline(self):
        if not self.expr or self.expr.streamlined:
            return self

        self.expr = self.expr.streamline()
        self.check_recursion()
        return self

    def check_recursion(self, seen=empty_tuple):
        if self in seen:
            raise RecursiveGrammarException(seen + (self,))
        if self.expr != None:
            self.expr.check_recursion(seen + (self,))

    def expecting(self):
        if self._in_expecting:
            return {}
        self._in_expecting = True
        try:
            if not self.expr:
                return {}
            return self.expr.expecting()
        finally:
            self._in_expecting = False

    def min_length(self):
        if self.min_length_cache is None and self.expr:
            self.min_length_cache = 0  # BREAK CYCLE
            try:
                return self.expr.min_length()
            finally:
                self.min_length_cache = None
        return 0

    @property
    def whitespace(self):
        if self.__in_whitespace:
            return None

        # Avoid infinite recursion by setting a temporary
        self.__in_whitespace = True
        try:
            return self.expr.whitespace
        finally:
            self.__in_whitespace = False

    def parse_impl(self, string, loc, do_actions=True):
        try:
            result = self.expr._parse(string, loc, do_actions)
            return ForwardResults(self, result.start, result.end, [result], result.failures)
        except Exception as cause:
            if is_null(self.expr):
                Log.warning(
                    "Ensure you have assigned a ParserElement (<<) to this Forward", cause=cause,
                )
            raise cause from None

    def __regex__(self):
        if self._in_regex:
            Log.error("recursion not supported")

        if not self.expr:
            Log.error("Forward is incomplete")

        try:
            self._in_regex = True
            return self.expr.__regex__()
        finally:
            self._in_regex = None

    def __str__(self):
        if self.parser_name:
            return self.parser_name

        if self._str:
            return self._str

        # Avoid infinite recursion
        self._str = "Forward: ..."
        try:
            self._str = "Forward: " + text(self.expr)[:1000]
        except Exception:
            pass
        return self._str

    def __call__(self, name):
        output = self.copy()
        output.token_name = name
        return output


class TokenConverter(ParseEnhancement):
    """
    Abstract subclass of `ParseExpression`, for converting parsed results.
    """

    __slots__ = []

    def __regex__(self):
        return self.expr.__regex__()


class Combine(TokenConverter):
    """
    Converter to concatenate all matching tokens to a single string.
    """

    Config = append_config(TokenConverter, "separator")

    def __init__(self, expr, separator=""):
        super(Combine, self).__init__(expr.streamline())
        self.set_config(separator=separator)

    def parse_impl(self, string, start, do_actions=True):
        try:
            result = self.expr.parse_impl(string, start, do_actions=do_actions)
            return ParseResults(
                self, start, result.end, [result.as_string(sep=self.parser_config.separator)], result.failures,
            )
        except ParseException as cause:
            raise ParseException(self, start, string, cause=cause)

    def streamline(self):
        if self.streamlined:
            return self

        expr = self.expr.streamline()
        if expr is self.expr:
            self.streamlined = True
            return self
        return Combine(expr, self.parser_config.separator).set_parser_name(self.parser_name)

    def expecting(self):
        return OrderedDict((k, [self]) for k in self.expr.expecting().keys())

    def min_length(self):
        return self.expr.min_length()

    def __regex__(self):
        return self.expr.__regex__()

    def __str__(self):
        if self.parser_name:
            return self.parser_name
        return text(self.expr)


class Group(TokenConverter):
    """
    MARK A CLOSED PARSE RESULT
    """

    __slots__ = []

    def __init__(self, expr):
        ParserElement.__init__(self)
        self.expr = whitespaces.CURRENT.normalize(expr)

    def is_annotated(self):
        return True


class Dict(Group):
    """
    Convert a list of tuples [(name, v1, v2, ...), ...]
    int dict-like lookup     {name: [v1, v2, ...], ...}

    mo-parsing uses the names of the ParserElement to name ParseResults,
    but this is a static naming scheme. Dict allows dynamic naming;
    Effectively defining new named ParserElements (called Annotations)
    at parse time
    """

    __slots__ = []

    def __init__(self, expr):
        Group.__init__(self, expr)
        self.parse_action.append(_dict_post_parse)


class OpenDict(TokenConverter):
    """
    Same as Dict, but not grouped: Open to previous (or subsequent) name: value pairs
    """

    __slots__ = []

    def __init__(self, expr):
        TokenConverter.__init__(self, expr)
        self.parse_action.append(_dict_post_parse)


def _dict_post_parse(tokens, loc, string):
    acc = tokens.tokens
    for a in list(acc):
        for tok in list(a):
            if not tok:
                continue
            if is_text(tok):
                new_tok = Annotation(tok, a.start, a.end, [])
            else:
                kv = list(tok)
                key = kv[0]
                value = kv[1:]
                new_tok = Annotation(text(key), tok.start, tok.end, value)
            acc.append(new_tok)

    return tokens


class Suppress(TokenConverter):
    """
    Converter for ignoring the results of a parsed expression.
    """

    __slots__ = []

    def __init__(self, expr):
        if isinstance(expr, text):
            expr = Literal(expr)
        TokenConverter.__init__(self, expr)
        self.parse_action.append(_suppress_post_parse)

    def suppress(self):
        return self

    def __regex__(self):
        return self.expr.__regex__()

    def __str__(self):
        if self.parser_name:
            return self.parser_name
        return text(self.expr)


def _suppress_post_parse(tokens, start, string):
    return ParseResults(tokens.type, tokens.start, tokens.end, [], tokens.failures)


class PrecededBy(LookAhead):
    """
    Lookbehind matching of the given parse expression.
    ``PrecededBy`` does not advance the parsing position within the
    input string, it only verifies that the specified parse expression
    matches prior to the current position.  ``PrecededBy`` always
    returns a null token list, but if a results name is defined on the
    given expression, it is returned.

    Parameters:

     - expr - expression that must match prior to the current parse
       location
     - retreat - (default= ``None``) - (int) maximum number of characters
       to lookbehind prior to the current parse location

    If the lookbehind expression is a string, Literal, Keyword, or
    a Word or CharsNotIn with a specified exact or maximum length, then
    the retreat parameter is not required. Otherwise, retreat must be
    specified to give a maximum number of characters to look back from
    the current parse position for a lookbehind match.
    """

    zero_length = True
    __slots__ = []
    Config = append_config(ParseEnhancement, "retreat", "exact")

    def __init__(self, expr, retreat=None):
        super(PrecededBy, self).__init__(expr)
        expr = self.expr

        if isinstance(expr, (Literal, Keyword, Char)):
            self.set_config(retreat=expr.min_length(), exact=True)
        elif isinstance(expr, (Word, CharsNotIn)):
            self.set_config(retreat=expr.min_length(), exact=False)
        elif expr.__class__.zero_length:
            self.set_config(retreat=0, exact=True)
        else:
            self.set_config(retreat=expr.min_length(), exact=False)

    def parse_impl(self, string, start=0, do_actions=True):
        if self.parser_config.exact:
            loc = start - self.parser_config.retreat
            if loc < 0:
                raise ParseException(self, start, string)
            ret = self.expr._parse(string, loc)
        else:
            # retreat specified a maximum lookbehind window, iterate
            test_expr = self.expr + StringEnd()
            instring_slice = string[:start]
            last_cause = ParseException(self, start, string)

            for offset in range(self.parser_config.retreat, start + 1):
                try:
                    ret = test_expr._parse(instring_slice, start - offset)
                    break
                except ParseException as cause:
                    last_cause = cause
            else:
                raise last_cause
        # return empty list of tokens, but preserve any defined results names

        ret.__class__ = Annotation
        return ParseResults(self, start, start, [ret], [])

    def __regex__(self):
        if self.parser_config.exact:
            return "*", f"(?<={self.expr.__regex__()[1]})"
        raise NotImplemented()


LookBehind = PrecededBy

export("mo_parsing.core", SkipTo)
export("mo_parsing.core", Many)
export("mo_parsing.core", ZeroOrMore)
export("mo_parsing.core", OneOrMore)
export("mo_parsing.core", Optional)
export("mo_parsing.core", NotAny)
export("mo_parsing.core", Suppress)
export("mo_parsing.core", Group)
export("mo_parsing.core",_suppress_post_parse)


export("mo_parsing.results", Group)
export("mo_parsing.results", Dict)
export("mo_parsing.results", Suppress)

export("mo_parsing.whitespaces", Empty)

export("mo_parsing.utils", Many)
