# encoding: utf-8
import json
from collections import OrderedDict
from operator import itemgetter

from mo_future import Iterable, text, generator_types
from mo_imports import export

from mo_parsing import whitespaces
from mo_parsing.core import ParserElement, _PendingSkip
from mo_parsing.enhancement import Optional, SkipTo, Many, LookBehind
from mo_parsing.exceptions import (
    ParseException,
    ParseSyntaxException,
)
from mo_parsing.results import ParseResults
from mo_parsing.tokens import Empty
from mo_parsing.utils import (
    empty_tuple,
    is_forward,
    regex_iso,
    Log,
    append_config,
    regex_caseless,
    regex_compile,
)
from mo_parsing.whitespaces import Whitespace


class ParseExpression(ParserElement):
    """Abstract subclass of ParserElement, for combining and
    post-processing parsed tokens.
    """

    __slots__ = ["exprs"]

    def __init__(self, exprs):
        super(ParseExpression, self).__init__()

        if isinstance(exprs, generator_types):
            exprs = list(exprs)
        elif not isinstance(exprs, ParserElement) and isinstance(exprs, Iterable):
            exprs = list(exprs)
        else:
            exprs = [exprs]

        self.exprs = [whitespaces.CURRENT.normalize(e) for e in exprs]
        for e in self.exprs:
            if is_forward(e):
                e.track(self)

    def expecting(self):
        output = OrderedDict()
        if not self.is_annotated():
            for e in self.exprs:
                expect = e.expecting()
                if not expect:
                    # NOT SURE WHAT THIS IS EXPECTING, BAIL
                    return {}
                for k, ee in expect.items():
                    output.setdefault(k, []).extend(ee)
        else:
            for e in self.exprs:
                expect = e.expecting()
                if not expect:
                    # NOT SURE WHAT THIS IS EXPECTING, BAIL
                    return {}
                for k, _ in expect.items():
                    output[k] = [self]
        return output

    def copy(self):
        output = ParserElement.copy(self)
        output.exprs = self.exprs
        return output

    def append(self, other):
        self.exprs.append(other)
        return self

    def leave_whitespace(self):
        """Extends ``leave_whitespace`` defined in base class, and also invokes ``leave_whitespace`` on
        all contained expressions."""
        with whitespaces.NO_WHITESPACE:
            output = self.copy()
            output.exprs = [e.leave_whitespace() for e in self.exprs]
            return output

    def streamline(self):
        if self.streamlined:
            return self
        self.streamlined = True

        # collapse nested And's of the form And(And(And(a, b), c), d) to And(a, b, c, d)
        # but only if there are no parse actions or resultsNames on the nested And's
        # (likewise for Or's and MatchFirst's)
        if not self.is_annotated() and not self.exprs:
            return Empty(self.parser_name)

        acc = []
        same = True
        clazz = self.__class__
        if clazz == Or:
            clazz = (
                Or,
                MatchFirst,
            )  # TODO: not correct, but allows merging of the two to a single longer list
        for e in self.exprs:
            f = e.streamline()
            same = same and f is e
            if f in acc and clazz in (Or, MatchFirst):
                same = False
                continue
            elif f.is_annotated():
                acc.append(f)
            elif isinstance(f, clazz):
                same = False
                acc.extend(f.exprs)
            else:
                acc.append(f)

        if same:
            return self

        output = self.copy()
        output.exprs = acc
        output.streamlined = True
        return output

    def __regex__(self):
        """
        RETURN TUPLE (operator, pattern) WHERE operator REPRESENTS PRECEDENCE
        """
        raise NotImplementedError

    def __call__(self, name):
        if not name:
            return self
        return ParserElement.__call__(self, name)


class And(ParseExpression):
    """
    Requires all given `ParseExpression` s to be found in the given order.
    Expressions may be separated by whitespace.
    May be constructed using the ``'+'`` operator.
    May also be constructed using the ``'-'`` operator, which will
    suppress backtracking.
    """

    __slots__ = []
    Config = append_config(ParseExpression, "whitespace")

    class SyntaxErrorGuard(Empty):
        def __init__(self, *args, **kwargs):
            with Whitespace(""):
                super(And.SyntaxErrorGuard, self).__init__(*args, **kwargs)
                self.parser_name = "-"

    def __init__(self, exprs, whitespace=None):
        if exprs and Ellipsis in exprs:
            tmp = []
            for i, expr in enumerate(exprs):
                if expr is Ellipsis:
                    if i < len(exprs) - 1:
                        skipto_arg = (Empty() + exprs[i + 1]).exprs[-1]
                        tmp.append(SkipTo(skipto_arg)("_skipped"))
                    else:
                        raise Exception(
                            "cannot construct And with sequence ending in ..."
                        )
                else:
                    tmp.append(expr)
            exprs[:] = tmp
        super(And, self).__init__(exprs)
        self.set_config(whitespace=whitespace or whitespaces.CURRENT)

    def streamline(self):
        if self.streamlined:
            return self
        if not self.exprs:
            return Empty(self.parser_name)
        if len(self.exprs) == 1 and not self.is_annotated():
            return self.exprs[0].streamline()

        # collapse any _PendingSkip's
        same = True
        exprs = self.exprs
        if any(
            isinstance(e, ParseExpression)
            and e.exprs
            and isinstance(e.exprs[-1], _PendingSkip)
            for e in exprs[:-1]
        ):
            same = False
            for i, e in enumerate(exprs[:-1]):
                if (
                    isinstance(e, ParseExpression)
                    and e.exprs
                    and isinstance(e.exprs[-1], _PendingSkip)
                ):
                    ee = e.exprs[-1] + exprs[i + 1]
                    e.exprs[-1] = ee
                    e.streamlined = False
                    exprs[i + 1] = None

        # streamline INDIVIDUAL EXPRESSIONS
        acc = []
        for e in exprs:
            if e is None:
                continue
            f = e.streamline()
            same = same and f is e
            if f.is_annotated():
                acc.append(f)
            elif (
                isinstance(f, And)
                and f.parser_config.whitespace is self.parser_config.whitespace
            ):
                same = False
                acc.extend(f.exprs)
            else:
                acc.append(f)

        if same:
            self.streamlined = True
            return self

        output = self.copy()
        output.exprs = acc
        output.streamlined = True
        return output

    def expecting(self):
        """
        RETURN A DICTIONARY OF ORDERED {regex: [patterns, ...]}
        WHERE
           regex IS a LIST OF (SEQUENCE OF NON-OVERLAPPING PATTERNS)
           patterns IS A LIST OF ParserElement
        :return:
        """
        if not self.exprs:
            return {}

        acc = OrderedDict()
        for e in self.exprs:
            expect = e.expecting()
            if not expect:
                return {}
            for k in expect.keys():
                acc[k] = [self]
            if e.min_length():
                break
        return acc

    def _min_length(self):
        return sum(e.min_length() for e in self.exprs)

    @property
    def whitespace(self):
        return self.parser_config.whitespace

    def parse_impl(self, string, start, do_actions=True):
        # pass False as last arg to _parse for first element, since we already
        # pre-parsed the string as part of our And pre-parsing
        encountered_syntax_error = False
        end = index = start
        acc = []
        failures = []
        for i, expr in enumerate(self.exprs):
            if end > index:
                if isinstance(expr, LookBehind):
                    index = end
                else:
                    index = self.parser_config.whitespace.skip(string, end)
            if isinstance(expr, And.SyntaxErrorGuard):
                encountered_syntax_error = True
                continue
            try:
                result = expr._parse(string, index, do_actions)
                failures.extend(result.failures)
                if not result and index == result.end and isinstance(result.type, Many) and result.type.parser_config.min_match == 0:
                    continue
                acc.append(result)
                end = result.end
            except ParseException as pe:
                failures.append(pe)
                if encountered_syntax_error:
                    raise ParseSyntaxException(
                        pe.expr, pe.loc, pe.string, cause=failures
                    ) from None
                else:
                    raise ParseException(
                        pe.expr, pe.loc, pe.string, cause=failures
                    ) from None

        return ParseResults(self, start, end, acc, failures)

    def __add__(self, other):
        if other is Ellipsis:
            return _PendingSkip(self)

        return And(
            [self, whitespaces.CURRENT.normalize(other)], whitespaces.CURRENT
        ).streamline()

    def check_recursion(self, seen=empty_tuple):
        subRecCheckList = seen + (self,)
        for e in self.exprs:
            e.check_recursion(subRecCheckList)
            if e.min_length():
                return

    def reverse(self):
        return And(
            [e.reverse() for e in self.exprs[::-1]], self.parser_config.whitespace
        )

    def __regex__(self):
        if self.whitespace is whitespaces.NO_WHITESPACE:
            return "+", "".join(regex_iso(*e.__regex__(), "+") for e in self.exprs)

        return (
            "+",
            regex_iso(*self.whitespace.__regex__(), "+").join(
                regex_iso(*e.__regex__(), "+") for e in self.exprs
            ),
        )

    def __str__(self):
        if self.parser_name:
            return self.parser_name

        subs = [text(e) for e in self.exprs]
        if all(len(s) == 1 for s in subs):
            return "".join(subs)
        else:
            return " + ".join(
                "{" + text(e) + "}" if isinstance(e, MatchFirst) else text(e)
                for e in self.exprs
            )


class Or(ParseExpression):
    """
    Requires that at least one `ParseExpression` is found. If
    two expressions match, the expression that matches the longest
    string will be used. May be constructed using the ``'^'``
    operator.
    """

    __slots__ = ["alternate"]

    def __init__(self, exprs):
        ParseExpression.__init__(self, exprs)
        self.alternate = self.exprs

    def copy(self):
        output = ParseExpression.copy(self)
        output.alternate = self.alternate
        return output

    def _min_length(self):
        return min(e.min_length() for e in self.exprs)

    def streamline(self):
        if self.streamlined:
            return self

        output = ParseExpression.streamline(self)

        if not isinstance(output, ParseExpression):
            return output
        if not output.is_annotated():
            if len(output.exprs) == 0:
                output = Empty()
            if len(output.exprs) == 1:
                output = output.exprs[0]
                if not isinstance(output, ParseExpression):
                    return output

        output.alternate = faster(output.exprs)

        output.streamlined = True
        output.check_recursion()
        return output

    @property
    def whitespace(self):
        return [e.whitespace for e in self.exprs]

    def parse_impl(self, string, start, do_actions=True):
        failures = []
        matches = []

        for e in self.alternate:
            if isinstance(e, Fast):
                for ee in e.get_short_list(string, start):
                    try:
                        end = ee._parse(string, start).end
                        matches.append((end, ee))
                    except ParseException as err:
                        failures.append(err)
            else:
                try:
                    end = e._parse(string, start).end
                    matches.append((end, e))
                except ParseException as err:
                    failures.append(err)

        if not matches:
            raise ParseException(
                self,
                start,
                string,
                msg="no defined alternatives to match",
                cause=failures,
            )
        if len(matches) == 1:
            _, expr = matches[0]
            result = expr._parse(string, start, do_actions)
            failures.extend(result.failures)
            return ParseResults(self, result.start, result.end, [result], failures)

        if matches:
            # re-evaluate all matches in descending order of length of match, in case attached actions
            # might change whether or how much they match of the input.
            matches.sort(key=itemgetter(0), reverse=True)

            if not do_actions:
                # no further conditions or parse actions to change the selection of
                # alternative, so the first match will be the best match
                _, expr = matches[0]
                result = expr._parse(string, start, do_actions)
                failures.extend(result.failures)
                return ParseResults(self, result.start, result.end, [result], failures)

            longest = -1, None
            for loc, expr in matches:
                if loc <= longest[0]:
                    # already have a longer match than this one will deliver, we are done
                    return longest

                try:
                    result = expr._parse(string, start, do_actions)
                except ParseException as err:
                    failures.append(err)
                else:
                    failures.extend(result.failures)
                    if result.end >= loc:
                        return ParseResults(
                            self, result.start, result.end, [result], failures
                        )
                    # didn't match as much as before
                    elif result.end > longest[0]:
                        longest = (
                            result.end,
                            ParseResults(
                                self, result.start, result.end, [result], failures,
                            ),
                        )

            if longest != (-1, None):
                return longest

    def check_recursion(self, seen=empty_tuple):
        seen_more = seen + (self,)
        for e in self.exprs:
            e.check_recursion(seen_more)

    def __regex__(self):
        return (
            "|",
            "|".join(
                regex_iso(*e.__regex__(), "|")
                for e in self.exprs
                if not isinstance(e, Empty)
            ),
        )

    def __str__(self):
        if self.parser_name:
            return self.parser_name

        return "{" + " ^ ".join(text(e) for e in self.exprs) + "}"


class MatchFirst(ParseExpression):
    """
    Requires that at least one `ParseExpression` is found. If
    two expressions match, the first one listed is the one that will
    match. May be constructed using the `|` operator.
    """

    __slots__ = ["alternate"]

    def __init__(self, exprs):
        ParseExpression.__init__(self, exprs)
        self.alternate = self.exprs

    def copy(self):
        output = ParseExpression.copy(self)
        output.alternate = self.alternate
        return output

    def _min_length(self):
        if self.exprs:
            return min(e.min_length() for e in self.exprs)
        else:
            Log.warning("expecting streamline")
            return 0

    @property
    def whitespace(self):
        return [e.whitespace for e in self.exprs]

    def parse_impl(self, string, start, do_actions=True):
        failures = []

        for e in self.alternate:
            try:
                result = e._parse(string, start, do_actions)
                failures.extend(result.failures)
                return ParseResults(self, result.start, result.end, [result], failures)
            except ParseException as cause:
                failures.append(cause)

        raise ParseException(self, start, string, cause=failures)

    def streamline(self):
        if self.streamlined:
            return self

        output = ParseExpression.streamline(self)

        if isinstance(output, Empty):
            return output
        if not output.is_annotated():
            if len(output.exprs) == 0:
                return Empty()
            if len(output.exprs) == 1:
                return output.exprs[0]

        output.alternate = faster(output.exprs)

        output.streamlined = True
        output.check_recursion()
        return output

    def check_recursion(self, seen=empty_tuple):
        seen_more = seen + (self,)
        for e in self.exprs:
            e.check_recursion(seen_more)

    def __or__(self, other):
        if other is Ellipsis:
            return _PendingSkip(Optional(self))

        return MatchFirst([self, whitespaces.CURRENT.normalize(other)]).streamline()

    def __ror__(self, other):
        return whitespaces.CURRENT.normalize(other) | self

    def __regex__(self):
        return (
            "|",
            "|".join(
                regex_iso(*e.__regex__(), "|")
                for e in self.exprs
                if not isinstance(e, Empty)
            ),
        )

    def __str__(self):
        if self.parser_name:
            return self.parser_name

        return " | ".join("{" + text(e) + "}" for e in self.exprs)


def faster(exprs):
    """
    BUILD A LOOKUP ARRAY TO MATCH ANY OF THE GIVEN exprs
    PERFORMS A REGEX, AND USES THE lower() CHARACTERS TO JUMP TO A SHORTLIST OF exprs THAT CAN MATCH
    :param exprs:
    :return: LESS EXPRESSIONS
    """

    if len(exprs) == 1:
        return exprs

    alternating = []
    # SOME NUMBER OF CONSTANT PATTERNS
    acc = []
    out = []
    has_expecting = True
    for o in exprs:
        p = o.expecting()
        if has_expecting:
            if p:
                acc.append(p)
                out.append(o)
            else:
                try:
                    e = Fast(acc)
                    alternating.append(e)
                except Exception as c:
                    alternating.extend(out)
                acc = []
                out = []
                alternating.append(o)
                has_expecting = False
        elif p:
            acc = [p]
            out = [o]
            has_expecting = True
        else:
            alternating.append(o)

    if has_expecting:
        try:
            e = Fast(acc)
            alternating.append(e)
        except Exception as cause:
            alternating.extend(out)
    return alternating


def _distinct(a, b):
    """
    ASSUME a != b
    RETURN MINIMUM length SO THAT a[:length] != b[:length]
    """
    ii = 1
    for aa, bb in zip(a, b):
        if aa != bb:
            return ii
        ii += 1
    return ii


class Fast(ParserElement):
    __slots__ = ["lookup", "regex", "all_keys"]

    def __init__(self, maps):
        ParserElement.__init__(self)

        all_keys = set()
        lookup = OrderedDict()
        for m in maps:
            for k, ee in m.items():
                k = k.lower()
                all_keys.add(k)
                lookup.setdefault(k, []).extend(ee)

        # patterns must be mutually exclusive to work
        items = list(lookup.items())
        if len(maps) - max(len(v) for k, v in items) <= 1:
            Log.error("not useful")

        compact = []
        for k, e in items:
            min_k = k
            # FIND SHORTEST PREFIX
            for kk, ee in items:
                if ee and min_k.startswith(kk):
                    min_k = kk
            # COLLECT
            acc = []
            for kk, ee in items:
                if kk.startswith(min_k):
                    acc.extend(ee)
                    ee.clear()
            if acc:
                compact.append((min_k, acc))
        if len(maps) - max(len(v) for k, v in compact) <= 1:
            Log.error("not useful")

        # patterns can be shortened so far as they remain exclusive
        shorter = [
            (k[:min_length], e)
            for k, e in sorted(compact, key=lambda p: p[0])
            for min_length in [max(_distinct(k, kk) for kk, _ in compact if kk != k)]
        ]

        self.lookup = {k: e for k, e in shorter}
        self.regex = regex_compile("|".join(regex_caseless(k) for k, _ in shorter))
        self.all_keys = list(sorted(all_keys))

    def get_short_list(self, string, start):
        """
        USE THE LOOKUP FEATURE TO FIND THE FEW ParserElements THAT CAN MATCH
        """
        found = self.regex.match(string, start)
        if found:
            index = found.group(0).lower()
            return self.lookup[index]
        return []

    def parse_impl(self, string, start, do_actions=True):
        found = self.regex.match(string, start)
        if found:
            index = found.group(0).lower()
            if index not in self.lookup:
                raise ParseException(
                    self, start, string, "expecting one of " + json.dumps(self.all_keys)
                )
            exprs = self.lookup[index]

            causes = []
            for e in exprs:
                try:
                    return e._parse(string, start, do_actions)
                except ParseException as cause:
                    causes.append(cause)

            raise ParseException(self, start, string, cause=causes)
        else:
            raise ParseException(
                self, start, string, "expecting one of " + json.dumps(self.all_keys)
            )


class MatchAll(ParseExpression):
    """
    Requires all given `ParseExpression` s to be found, but in
    any order. Expressions may be separated by whitespace.

    May be constructed using the ``'&'`` operator.
    """

    __slots__ = []
    Config = append_config(ParseExpression, "min_match", "max_match", "whitespace")

    def __init__(self, exprs):
        """
        :param exprs: The expressions to be matched
        :param mins: list of integers indincating any minimums
        """
        ParseExpression.__init__(self, exprs)
        self.set_config(
            whitespace=whitespaces.CURRENT,
            min_match=[
                e.parser_config.min_match if isinstance(e, Many) else 1 for e in exprs
            ],
            max_match=[
                e.parser_config.max_match if isinstance(e, Many) else 1 for e in exprs
            ],
        )

    def streamline(self):
        if self.streamlined:
            return self
        output = ParseExpression.streamline(self)
        output.set_config(
            min_match=[
                e.parser_config.min_match if isinstance(e, Many) else 1
                for e in output.exprs
            ],
            max_match=[
                e.parser_config.max_match if isinstance(e, Many) else 1
                for e in output.exprs
            ],
        )
        return output

    def _min_length(self):
        # TODO: MAY BE TOO CONSERVATIVE, WE MAY BE ABLE TO PROVE self CAN CONSUME A CHARACTER
        return min(e.min_length() for e in self.exprs)

    @property
    def whitespace(self):
        return [e.whitespace for e in self.exprs]

    def parse_impl(self, string, start, do_actions=True):
        end = start
        match_order = []
        todo = list(zip(
            self.exprs, self.parser_config.min_match, self.parser_config.max_match
        ))
        count = [0] * len(self.exprs)
        failures = []
        while todo:
            for i, (c, (e, mi, ma)) in enumerate(zip(count, todo)):
                try:
                    result = e._parse(string, end)
                    failures.extend(result.failures)
                    loc = result.end
                    if loc == end:
                        continue
                    end = self.parser_config.whitespace.skip(string, loc)
                    c2 = count[i] = c + 1
                    if c2 >= ma:
                        del todo[i]
                        del count[i]
                    match_order.append(e)
                    break
                except ParseException as pe:
                    failures.append(pe)
            else:
                break

        for c, (e, mi, ma) in zip(count, todo):
            if c < mi:
                raise ParseException(
                    self,
                    start,
                    string,
                    "Missing minimum (%i) more required elements (%s)" % (mi, e),
                    cause=failures,
                )

        found = set(id(m) for m in match_order)
        missing = [
            e
            for e, mi in zip(self.exprs, self.parser_config.min_match)
            if id(e) not in found and mi > 0
        ]
        if missing:
            missing = ", ".join(text(e) for e in missing)
            raise ParseException(
                self,
                start,
                string,
                f"Missing one or more required elements ({missing})",
                failures,
            )

        # add any unmatched Optionals, in case they have default values defined
        match_order += [e for e in self.exprs if id(e) not in found]

        if not match_order:
            return ParseResults(self, start, start, [], failures)

        # TODO: CAN WE AVOID THIS RE-PARSE?
        results = []
        end = start
        for e in match_order:
            result = e._parse(string, end, do_actions)
            end = self.parser_config.whitespace.skip(string, result.end)
            results.append(result)

        return ParseResults(self, results[0].start, results[-1].end, results, failures)

    def __str__(self):
        if self.parser_name:
            return self.parser_name

        return "{" + " & ".join(text(e) for e in self.exprs) + "}"


export("mo_parsing.core", And)
export("mo_parsing.core", Or)
export("mo_parsing.core", MatchAll)
export("mo_parsing.core", MatchFirst)
export("mo_parsing.exceptions", MatchFirst)
