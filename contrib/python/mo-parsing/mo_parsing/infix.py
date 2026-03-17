# encoding: utf-8
import re
import warnings

from mo_future import text, Iterable
from mo_imports import delay_import

from mo_parsing import whitespaces
from mo_parsing.enhancement import (
    Combine,
    Forward,
    Group,
    Suppress,
    ZeroOrMore,
)
from mo_parsing.expressions import MatchFirst, Or
from mo_parsing.results import ParseResults, NO_PARSER
from mo_parsing.tokens import (
    CaselessKeyword,
    CaselessLiteral,
    Keyword,
    NoMatch,
    Literal,
    Empty,
    Log,
)
from mo_parsing.utils import regex_range, wrap_parse_action, enlist

Regex = delay_import("mo_parsing.regex.Regex")


def delimited_list(expr, separator=",", combine=False):
    """
    PARSE DELIMITED LIST OF expr
    Example::

        delimited_list(Word(alphas)).parse_string("aa,bb,cc") # -> ['aa', 'bb', 'cc']
        delimited_list(Word(hexnums), delim=':', combine=True).parse_string("AA:BB:CC:DD:EE") # -> ['AA:BB:CC:DD:EE']
    """
    if combine:
        return Combine(expr + ZeroOrMore(separator + expr, whitespaces.CURRENT))
    else:
        return expr + ZeroOrMore(Suppress(separator) + expr, whitespaces.CURRENT)


def one_of(strs, caseless=False, as_keyword=False):
    """
    Helper to quickly define a set of alternative Literals, and makes
    sure to do longest-first testing when there is a conflict,
    regardless of the input order, but returns
    a `MatchFirst` for best performance.

    Parameters:

     - strs - a string of space-delimited literals, or a collection of
       string literals
     - caseless - (default= ``False``) - treat all literals as caseless
     - as_keyword - (default=``False``) - enforce Keyword-style matching on the
       generated expressions
    """
    if isinstance(caseless, str):
        Log.error(
            "More than one string argument passed to one_of, pass choices as a list or"
            " space-delimited string"
        )

    if caseless:
        isequal = lambda a, b: a.upper() == b.upper()
        masks = lambda a, b: b.upper().startswith(a.upper())
        parseElementClass = CaselessKeyword if as_keyword else CaselessLiteral
    else:
        isequal = lambda a, b: a == b
        masks = lambda a, b: b.startswith(a)
        parseElementClass = Keyword if as_keyword else Literal

    symbols = []
    if isinstance(strs, text):
        symbols = strs.split()
    elif isinstance(strs, Iterable):
        symbols = list(strs)
    else:
        warnings.warn(
            "Invalid argument to one_of, expected string or iterable",
            SyntaxWarning,
            stacklevel=2,
        )
    if not symbols:
        return NoMatch()

    if not as_keyword:
        # if not producing keywords, need to reorder to take care to avoid masking
        # longer choices with shorter ones
        i = 0
        while i < len(symbols) - 1:
            cur = symbols[i]
            for j, other in enumerate(symbols[i + 1 :]):
                if isequal(other, cur):
                    del symbols[i + j + 1]
                    break
                elif masks(cur, other):
                    del symbols[i + j + 1]
                    symbols.insert(i, other)
                    break
            else:
                i += 1

    if caseless or as_keyword:
        return MatchFirst(parseElementClass(sym) for sym in symbols).streamline()

    # CONVERT INTO REGEX
    singles = [s for s in symbols if len(s) == 1]
    rest = list(sorted([s for s in symbols if len(s) != 1], key=lambda s: -len(s)))

    acc = []
    acc.extend(re.escape(sym) for sym in rest)
    if singles:
        acc.append(regex_range("".join(singles)))
    regex = "|".join(acc)

    return Regex(regex).streamline()


LEFT_ASSOC = object()
RIGHT_ASSOC = object()
_no_op = Empty().suppress()


def infix_notation(
    base_expr, spec, lpar=Suppress(Literal("(")), rpar=Suppress(Literal(")"))
):
    """
    :param base_expr: expression representing the most basic element for the
       nested
    :param spec: list of tuples, one for each operator precedence level
       in the expression grammar; each tuple is of the form ``(op_expr,
       numTerms, rightLeftAssoc, parse_action)``, where:

       - op_expr is the mo_parsing expression for the operator; may also
         be a string, which will be converted to a Literal; if numTerms
         is 3, op_expr is a tuple of two expressions, for the two
         operators separating the 3 terms
       - numTerms is the number of terms for this operator (must be 1,
         2, or 3)
       - rightLeftAssoc is the indicator whether the operator is right
         or left associative, using the mo_parsing-defined constants
         ``RIGHT_ASSOC`` and ``LEFT_ASSOC``.
       - parse_action is the parse action to be associated with
         expressions matching this operator expression
    :param lpar: expression for matching left-parentheses
       (default= ``Suppress('(')``)
    :param rpar: expression for matching right-parentheses
       (default= ``Suppress(')')``)
    :return: ParserElement
    """

    all_op = {}

    def norm(op):
        if op == None:
            op = _no_op
        output = all_op.get(id(op))
        if output:
            return output

        def record_self(tok):
            ParseResults(tok.type, tok.start, tok.end, [tok.type.parser_name], [])

        output = whitespaces.CURRENT.normalize(op)
        is_suppressed = isinstance(output, Suppress)
        if is_suppressed:
            output = output.expr
        output = output / record_self
        all_op[id(op)] = is_suppressed, output
        return is_suppressed, output

    op_list = []
    """
    SCRUBBED LIST OF OPERATORS
    * expr - used exclusively for ParseResult(expr, [...]), not used to match
    * op - used to match 
    * arity - same
    * assoc - same
    * parse_actions - same
    """

    for oper_def in spec:
        op, arity, assoc, rest = oper_def[0], oper_def[1], oper_def[2], oper_def[3:]
        parse_actions = list(map(wrap_parse_action, enlist(rest[0]))) if rest else []
        if arity == 1:
            is_suppressed, op = norm(op)
            if assoc == RIGHT_ASSOC:
                op_list.append((
                    Group(base_expr + op),
                    op,
                    is_suppressed,
                    arity,
                    assoc,
                    parse_actions,
                ))
            else:
                op_list.append((
                    Group(op + base_expr),
                    op,
                    is_suppressed,
                    arity,
                    assoc,
                    parse_actions,
                ))
        elif arity == 2:
            is_suppressed, op = norm(op)
            op_list.append((
                Group(base_expr + op + base_expr),
                op,
                is_suppressed,
                arity,
                assoc,
                parse_actions,
            ))
        elif arity == 3:
            is_suppressed, op = zip(norm(op[0]), norm(op[1]))
            op_list.append((
                Group(base_expr + op[0] + base_expr + op[1] + base_expr),
                op,
                is_suppressed,
                arity,
                assoc,
                parse_actions,
            ))
    op_list = tuple(op_list)

    def record_op(op):
        def output(tokens):
            return ParseResults(NO_PARSER, tokens.start, tokens.end, [(tokens, op)], [])

        return output

    prefix_ops = MatchFirst([
        op / record_op(op)
        for expr, op, is_suppressed, arity, assoc, pa in op_list
        if arity == 1 and assoc == RIGHT_ASSOC
    ])
    suffix_ops = MatchFirst([
        op / record_op(op)
        for expr, op, is_suppressed, arity, assoc, pa in op_list
        if arity == 1 and assoc == LEFT_ASSOC
    ])
    ops = Or([
        op_part / record_op(op_part)
        for op_part in set(
            op_part
            for expr, op, is_suppressed, arity, assoc, pa in op_list
            if arity > 1
            for op_part in (op if isinstance(op, tuple) else [op])
        )
    ])

    def make_tree(tokens, loc, string):
        flat_tokens = list(tokens)
        num = len(op_list)
        op_index = 0
        while len(flat_tokens) > 1 and op_index < num:
            expr, op, is_suppressed, arity, assoc, parse_actions = op_list[op_index]
            if arity == 1:
                if assoc == RIGHT_ASSOC:
                    # PREFIX OPERATOR -3
                    todo = list(reversed(list(enumerate(flat_tokens[:-1]))))
                    for i, (r, o) in todo:
                        if o == op:
                            tok = flat_tokens[i + 1][0]
                            if is_suppressed:
                                result = ParseResults(
                                    expr, tok.start, tok.end, (tok,), []
                                )
                            else:
                                result = ParseResults(
                                    expr, r.start, tok.end, (r, tok), []
                                )
                            break
                    else:
                        op_index += 1
                        continue
                else:
                    # SUFFIX OPERATOR 3!
                    todo = list(enumerate(flat_tokens[1:]))
                    for i, (r, o) in todo:
                        if o == op:
                            tok = flat_tokens[i][0]
                            if is_suppressed:
                                result = ParseResults(
                                    expr, tok.start, tok.end, (tok,), []
                                )
                            else:
                                result = ParseResults(
                                    expr, tok.start, r.end, (tok, r,), []
                                )
                            break
                    else:
                        op_index += 1
                        continue
            elif arity == 2:
                todo = list(enumerate(flat_tokens[1:-1]))
                if assoc == RIGHT_ASSOC:
                    todo = list(reversed(todo))

                for i, (r, o) in todo:
                    if o == op:
                        if is_suppressed:
                            result = ParseResults(
                                expr,
                                flat_tokens[i][0].start,
                                flat_tokens[i + 2][0].end,
                                (flat_tokens[i][0], flat_tokens[i + 2][0]),
                                [],
                            )
                        else:
                            result = ParseResults(
                                expr,
                                flat_tokens[i][0].start,
                                flat_tokens[i + 2][0].end,
                                (flat_tokens[i][0], r, flat_tokens[i + 2][0]),
                                [],
                            )
                        break
                else:
                    op_index += 1
                    continue

            else:  # arity==3
                todo = list(enumerate(flat_tokens[1:-3]))
                if assoc == RIGHT_ASSOC:
                    todo = list(reversed(todo))

                for i, (r0, o0) in todo:
                    if o0 == op[0]:
                        r1, o1 = flat_tokens[i + 3]
                        if o1 == op[1]:
                            seq = [
                                flat_tokens[i][0],
                                flat_tokens[i + 2][0],
                                flat_tokens[i + 4][0],
                            ]
                            s0, s1 = is_suppressed
                            if not s1:
                                seq.insert(2, r1)
                            if not s0:
                                seq.insert(1, r0)

                            result = ParseResults(
                                expr, seq[0].start, seq[-1].end, seq, []
                            )
                            break
                else:
                    op_index += 1
                    continue

            for p in parse_actions:
                result = p(result, -1, string)
            offset = (0, 2, 3, 5)[arity]
            flat_tokens[i : i + offset] = [(result, (expr,))]
            op_index = 0

        result = flat_tokens[0][0]
        result.end = tokens.end
        result.failures = tokens.failures
        return result

    flat = Forward()
    iso = lpar.suppress() + flat + rpar.suppress()
    atom = (base_expr | iso) / record_op(base_expr)
    decorated = ZeroOrMore(prefix_ops) + atom + ZeroOrMore(suffix_ops)
    flat << ((decorated + ZeroOrMore(ops + decorated)) / make_tree).streamline()

    return flat.streamline()
