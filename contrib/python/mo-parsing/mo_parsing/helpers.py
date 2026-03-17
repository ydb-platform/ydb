# encoding: utf-8
import re
from datetime import datetime

from mo_future import text

from mo_parsing import whitespaces
from mo_parsing.core import add_reset_action
from mo_parsing.enhancement import (
    Combine,
    Dict,
    Forward,
    Group,
    OneOrMore,
    Optional,
    Suppress,
    TokenConverter,
    ZeroOrMore,
    OpenDict,
    Many,
)
from mo_parsing.exceptions import ParseException
from mo_parsing.expressions import And
from mo_parsing.infix import delimited_list
from mo_parsing.regex import Regex
from mo_parsing.results import ParseResults, Annotation
from mo_parsing.tokens import (
    CharsNotIn,
    Empty,
    Keyword,
    LineEnd,
    Word,
    Literal,
    AnyChar,
    Char,
)
from mo_parsing.utils import (
    alphanums,
    alphas,
    col,
    hexnums,
    nums,
    printables,
    Log,
)
from mo_parsing.whitespaces import Whitespace, STANDARD_WHITESPACE, NO_WHITESPACE


def QuotedString(
    quote_char,
    esc_char=None,
    esc_quote=None,
    multiline=False,
    unquote_results=True,
    end_quote_char="",
    convert_whitespace_escape=True,
):
    r"""
    Token for matching strings that are delimited by quoting characters.

    Defined with the following parameters:

        - quote_char - string of one or more characters defining the
          quote delimiting string
        - esc_char - character to escape quotes, typically backslash
          (default= ``None``)
        - esc_quote - special quote sequence to escape an embedded quote
          string (such as SQL's ``""`` to escape an embedded ``"``)
          (default= ``None``)
        - multiline - boolean indicating whether quotes can span
          multiple lines (default= ``False``)
        - unquote_results - boolean indicating whether the matched text
          should be unquoted (default= ``True``)
        - end_quote_char - string of one or more characters defining the
          end of the quote delimited string (default= ``None``  => same as
          quote_char)
        - convertWhitespaceEscapes - convert escaped whitespace
          (``'\t'``, ``'\n'``, etc.) to actual whitespace
          (default= ``True``)

    """
    quote_char = quote_char.strip()
    end_quote_char = end_quote_char.strip() or quote_char

    if not quote_char:
        Log.error("quote_char cannot be the empty string")
    if not end_quote_char:
        Log.error("end_quote_char cannot be the empty string")

    excluded = Literal(end_quote_char)

    if multiline:
        anychar = AnyChar()
    else:
        anychar = Char(exclude="\n")
        excluded |= Char("\r\n")

    with whitespaces.NO_WHITESPACE:
        included = ~Literal(end_quote_char) + anychar

        if esc_quote:
            included = Literal(esc_quote) | included
        if esc_char:
            excluded |= Literal(esc_char)
            included = esc_char + Char(printables) | included
            esc_char_replace_pattern = re.escape(esc_char) + "(.)"

        prec, pattern = (
            Literal(quote_char) + ((~excluded + anychar) | included)[0:]
        ).__regex__()
        # IMPORTANT: THE end_quote_char IS OUTSIDE THE Regex BECAUSE OF PATHOLOGICAL BACKTRACKING
        output = Combine(Regex(pattern) + Literal(end_quote_char))

    def post_parse(tokens):
        ret = tokens[0]
        if unquote_results:
            # strip off quotes
            ret = ret[len(quote_char) : -len(end_quote_char)]

            if isinstance(ret, text):
                # replace escaped whitespace
                if "\\" in ret and convert_whitespace_escape:
                    ws_map = {
                        r"\t": "\t",
                        r"\n": "\n",
                        r"\f": "\f",
                        r"\r": "\r",
                    }
                    for wslit, wschar in ws_map.items():
                        ret = ret.replace(wslit, wschar)

                # replace escaped characters
                if esc_char:
                    ret = re.sub(esc_char_replace_pattern, r"\g<1>", ret)

                # replace escaped quotes
                if esc_quote:
                    ret = ret.replace(esc_quote, end_quote_char)

        return ParseResults(tokens.type, tokens.start, tokens.end, [ret], [])

    return (output / post_parse).streamline()


dblQuotedString = Combine(
    #       0         1         2         3         4         5
    #       012345678901234567890123456789012345678901234567890123456789
    Regex(r'"(?:[^"\n\r\\]|(?:"")|(?:\\(?:[^x]|x[0-9a-fA-F]+)))*')
    + '"'
).set_parser_name("string enclosed in double quotes")
sglQuotedString = Combine(
    Regex(r"'(?:[^'\n\r\\]|(?:'')|(?:\\(?:[^x]|x[0-9a-fA-F]+)))*") + "'"
).set_parser_name("string enclosed in single quotes")
quoted_string = Combine(
    Regex(r'"(?:[^"\n\r\\]|(?:"")|(?:\\(?:[^x]|x[0-9a-fA-F]+)))*') + '"'
    | Regex(r"'(?:[^'\n\r\\]|(?:'')|(?:\\(?:[^x]|x[0-9a-fA-F]+)))*") + "'"
).set_parser_name("quoted_string using single or double quotes")
unicode_string = Combine(
    Literal("u") + quoted_string
).set_parser_name("unicode string literal")


def counted_array(expr, int_expr=None):
    """Helper to define a counted list of expressions.

    This helper defines a pattern of the form::

        integer expr expr expr...

    where the leading integer tells how many expr expressions follow.
    The matched tokens returns the array of expr tokens as a list - the
    leading count token is suppressed.

    If ``int_expr`` is specified, it should be a mo_parsing expression
    that produces an integer value.

    Example::

        counted_array(Word(alphas)).parse_string('2 ab cd ef')  # -> ['ab', 'cd']

        # in this parser, the leading integer value is given in binary,
        # '10' indicating that 2 values are in the array
        binary_constant = Word('01')/ lambda t: int(t[0], 2)
        counted_array(Word(alphas), int_expr=binary_constant).parse_string('10 ab cd ef')  # -> ['ab', 'cd']
    """
    if int_expr is None:
        int_expr = Word(nums) / (lambda t: int(t[0]))

    array_expr = Forward()

    def countFieldParseAction(t, l, s):
        n = t[0]
        array_expr << Group(Many(expr, exact=n, whitespace=whitespaces.CURRENT))
        return []

    int_expr = (
        int_expr
        .set_parser_name("array_len")
        .add_parse_action(countFieldParseAction, callDuringTry=True)
    )
    return (int_expr + array_expr).set_parser_name("(len) " + text(expr) + "...")


def _flatten(L):
    ret = []
    for i in L:
        if isinstance(i, list):
            ret.extend(_flatten(i))
        else:
            ret.append(i)
    return ret


def matchPreviousLiteral(expr):
    """
    Helper to define an expression that is indirectly defined from
    the tokens matched in a previous expression, that is, it looks for
    a 'repeat' of a previous expression.  For example::

        first = Word(nums)
        second = matchPreviousLiteral(first)
        match_expr = first + ":" + second

    will match ``"1:1"``, but not ``"1:2"``.  Because this
    matches a previous literal, will also match the leading
    ``"1:1"`` in ``"1:10"``. If this is not desired, use
    `matchPreviousExpr`. Do *not* use with packrat parsing
    enabled.
    """
    rep = Forward()

    def copyTokenToRepeater(t, l, s):
        if t:
            if len(t) == 1:
                rep << t[0]
            else:
                # flatten t tokens
                tflat = _flatten(t)
                rep << And(Literal(tt) for tt in tflat)
        else:
            rep << Empty()

    expr.add_parse_action(copyTokenToRepeater, callDuringTry=True)
    rep.set_parser_name("(prev) " + text(expr))
    return rep


def matchPreviousExpr(expr):
    """Helper to define an expression that is indirectly defined from
    the tokens matched in a previous expression, that is, it looks for
    a 'repeat' of a previous expression.  For example::

        first = Word(nums)
        second = matchPreviousExpr(first)
        match_expr = first + ":" + second

    will match ``"1:1"``, but not ``"1:2"``.  Because this
    matches by expressions, will *not* match the leading ``"1:1"``
    in ``"1:10"``; the expressions are evaluated first, and then
    compared, so ``"1"`` is compared with ``"10"``. Do *not* use
    with packrat parsing enabled.
    """
    rep = Forward()
    e2 = expr.copy()
    rep <<= e2

    def copyTokenToRepeater(t, l, s):
        match_tokens = _flatten(t)

        def mustMatchTheseTokens(t, l, s):
            these_tokens = _flatten(t)
            if these_tokens != match_tokens:
                raise ParseException("", 0, "")

        rep.add_parse_action(mustMatchTheseTokens, callDuringTry=True)

    expr.add_parse_action(copyTokenToRepeater, callDuringTry=True)
    rep.set_parser_name("(prev) " + text(expr))
    return rep


def dict_of(key, value):
    """Helper to easily and clearly define a dictionary by specifying
    the respective patterns for the key and value.  Takes care of
    defining the `Dict`, `ZeroOrMore`, and
    `Group` tokens in the proper order.  The key pattern
    can include delimiting markers or punctuation, as long as they are
    suppressed, thereby leaving the significant key text.  The value
    pattern can include named results, so that the `Dict` results
    can include named token fields.

    Example::

        text = "shape: SQUARE posn: upper left color: light blue texture: burlap"
        attr_expr = (label + Suppress(':') + OneOrMore(data_word, stop_on=label)/ ' '.join)
        print(OneOrMore(attr_expr).parse_string(text))

        attr_label = label
        attr_value = Suppress(':') + OneOrMore(data_word, stop_on=label)/ ' '.join

        # similar to Dict, but simpler call format
        result = dict_of(attr_label, attr_value).parse_string(text)
        print(result)
        print(result['shape'])
        print(result.shape)  # object attribute access works too
        print(result)

    prints::

        [['shape', 'SQUARE'], ['posn', 'upper left'], ['color', 'light blue'], ['texture', 'burlap']]
        - color: light blue
        - posn: upper left
        - shape: SQUARE
        - texture: burlap
        SQUARE
        SQUARE
        {'color': 'light blue', 'shape': 'SQUARE', 'posn': 'upper left', 'texture': 'burlap'}
    """
    return Dict(OneOrMore(Group(key + value)))


def originalTextFor(expr, as_string=True):
    """Helper to return the original, untokenized text for a given
    expression.  Useful to restore the parsed fields of an HTML start
    tag into the raw tag text itself, or to revert separate tokens with
    intervening whitespace back to the original matching input text. By
    default, returns astring containing the original parsed text.

    If the optional ``as_string`` argument is passed as
    ``False``, then the return value is
    a `ParseResults` containing any results names that
    were originally matched, and a single token containing the original
    matched text from the input string.  So if the expression passed to
    `originalTextFor` contains expressions with defined
    results names, you must set ``as_string`` to ``False`` if you
    want to preserve those results name values.

    Example::

        src = "this is test <b> bold <i>text</i> </b> normal text "
        for tag in ("b", "i"):
            opener, closer = makeHTMLTags(tag)
            patt = originalTextFor(opener + SkipTo(closer) + closer)
            print(patt.search_string(src)[0])

    prints::

        ['<b> bold <i>text</i> </b>']
        ['<i>text</i>']
    """
    loc_marker = Empty() / (lambda _, l: l)
    match_expr = (
        loc_marker("_original_start") + Group(expr) + loc_marker("_original_end")
    )
    match_expr = match_expr / extract_text
    return match_expr


def extract_text(tokens, loc, string):
    start, d, end = tokens
    content = string[start:end]
    annotations = [Annotation(k, v[0].start, v[-1].end, v) for k, v in d.items()]
    return ParseResults(d.type, start, end, [content] + annotations, [])


def ungroup(expr):
    """Helper to undo mo_parsing's default grouping of And expressions,
    even if all but one are non-empty.
    """
    return TokenConverter(expr) / (lambda t: t[0])


def located_expr(expr):
    """Helper to decorate a returned token with its starting and ending
    locations in the input string.

    This helper adds the following results names:

     - locn_start = location where matched expression begins
     - locn_end = location where matched expression ends
     - value = the actual parsed results

    Be careful if the input text contains ``<TAB>`` characters, you
    may want to call `ParserElement.parseWithTabs`

    Example::

        wd = Word(alphas)
        for match in located_expr(wd).search_string("ljsdf123lksdjjf123lkkjj1222"):
            print(match)

    prints::

        [[0, 'ljsdf', 5]]
        [[8, 'lksdjjf', 15]]
        [[18, 'lkkjj', 23]]
    """
    locator = Empty() / (lambda t, l, s: l)
    return Group(locator("locn_start") + Group(expr)("value") + locator("locn_end"))


def nested_expr(opener="(", closer=")", content=None, ignore_expr=quoted_string):
    """Helper method for defining nested lists enclosed in opening and
    closing delimiters ("(" and ")" are the default).

    Parameters:
     - opener - opening character for a nested list
       (default= ``"("``); can also be a mo_parsing expression
     - closer - closing character for a nested list
       (default= ``")"``); can also be a mo_parsing expression
     - content - expression for items within the nested lists
       (default= ``None``)
     - ignore_expr - expression for ignoring opening and closing
       delimiters (default= `quoted_string`)

    If an expression is not provided for the content argument, the
    nested expression will capture all whitespace-delimited content
    between delimiters as a list of separate values.

    Use the ``ignore_expr`` argument to define expressions that may
    contain opening or closing characters that should not be treated as
    opening or closing characters for nesting, such as quoted_string or
    a comment expression.  Specify multiple expressions using an
    `Or` or `MatchFirst`. The default is
    `quoted_string`, but if no expressions are to be ignored, then
    pass ``None`` for this argument.

    """
    if opener == closer:
        raise ValueError("opening and closing strings cannot be the same")
    if content is None:
        if not isinstance(opener, text) or not isinstance(closer, text):
            raise ValueError(
                "opening and closing arguments must be strings if no content expression"
                " is given"
            )

        ignore_chars = whitespaces.CURRENT.white_chars
        with Whitespace(""):

            def scrub(t):
                return t[0].strip()

            if len(opener) == 1 and len(closer) == 1:
                if ignore_expr is not None:
                    content = (
                        Combine(OneOrMore(
                            ~ignore_expr
                            + CharsNotIn(
                                opener + closer + "".join(ignore_chars), exact=1,
                            )
                        ))
                        / scrub
                    )
                else:
                    content = (
                        Empty + CharsNotIn(opener + closer + "".join(ignore_chars))
                    ) / scrub
            else:
                if ignore_expr is not None:
                    content = (
                        Combine(OneOrMore(
                            ~ignore_expr
                            + ~Literal(opener)
                            + ~Literal(closer)
                            + CharsNotIn(ignore_chars, exact=1)
                        ))
                        / scrub
                    )
                else:
                    content = (
                        Combine(OneOrMore(
                            ~Literal(opener)
                            + ~Literal(closer)
                            + CharsNotIn(ignore_chars, exact=1)
                        ))
                        / scrub
                    )
    ret = Forward()
    if ignore_expr is not None:
        ret <<= Group(
            Suppress(opener)
            + ZeroOrMore(ignore_expr | ret | content)
            + Suppress(closer)
        )
    else:
        ret <<= Group(Suppress(opener) + ZeroOrMore(ret | content) + Suppress(closer))
    ret.set_parser_name("nested %s%s expression" % (opener, closer))
    return ret


def matchOnlyAtCol(n):
    """Helper method for defining parse actions that require matching at
    a specific column in the input text.
    """

    def verify_col(strg, locn, toks):
        if col(locn, strg) != n:
            raise ParseException(strg, locn, "matched token not at column %d" % n)

    return verify_col


def remove_quotes(t, l, s):
    """Helper parse action for removing quotation marks from parsed
    quoted strings.

    Example::

        # by default, quotation marks are included in parsed results
        quoted_string.parse_string("'Now is the Winter of our Discontent'") # -> ["'Now is the Winter of our Discontent'"]

        # use removeQuotes to strip quotation marks from parsed results
        quoted_string/ removeQuotes
        quoted_string.parse_string("'Now is the Winter of our Discontent'") # -> ["Now is the Winter of our Discontent"]
    """
    return t[0][1:-1]


def token_map(func, *args):
    """
    APPLY func OVER ALL GIVEN TOKENS
    :param func: ACCEPT ParseResults
    :param args: ADDITIONAL PARAMETERS TO func
    :return:  map(func(e), token)
    """

    def pa(t, l, s):
        return [func(tokn, *args) for tokn in t]

    try:
        func_name = getattr(func, "__name__", getattr(func, "__class__").__name__)
    except Exception:
        func_name = str(func)
    pa.__name__ = func_name

    return pa


upcase_tokens = token_map(lambda t: text(t).upper())
"""(Deprecated) Helper parse action to convert tokens to upper case.
Deprecated in favor of `upcase_tokens`"""

downcase_tokens = token_map(lambda t: text(t).lower())
"""(Deprecated) Helper parse action to convert tokens to lower case.
Deprecated in favor of `downcase_tokens`"""


def makeHTMLTags(tag, suppress_LT=Suppress("<"), suppress_GT=Suppress(">")):
    """Helper to construct opening and closing tag expressions for HTML,
    given a tag name. Matches tags in either upper or lower case,
    attributes with namespaces and with quoted or unquoted values.
    """
    if isinstance(tag, text):
        resname = tag
        tag = Keyword(tag, caseless=True)
    else:
        resname = tag.parser_name

    tagAttrName = Word(alphas, alphanums + "_-:")
    tagAttrValue = quoted_string / remove_quotes | Word(printables, exclude=">")
    simpler_name = "".join(resname.replace(":", " ").title().split())

    with STANDARD_WHITESPACE:
        open_tag = (
            (
                suppress_LT
                + tag("tag")
                + OpenDict(ZeroOrMore(Group(
                    tagAttrName / downcase_tokens
                    + Optional(Suppress("=") + tagAttrValue)
                )))
                + Optional("/", default=[False])("empty")
                / (lambda t, l, s: t[0] == "/")
                + suppress_GT
            )
            .set_token_name("start" + simpler_name)
            .set_parser_name("<%s>" % resname)
        )

        close_tag = (
            Combine(Literal("</") + tag + ">")
            .set_token_name("end" + simpler_name)
            .set_parser_name("</%s>" % resname)
        )

    # openTag.tag = resname
    # closeTag.tag = resname
    # openTag.tag_body = SkipTo(closeTag)

    return open_tag, close_tag


def with_attribute(**attr):
    """
    Verify attributes have given value, or at least exists (using with_attribute.ANY_VALUE)
    :param attr:  Expecting named parameters set to the expected value
    :return:  Raise an exception if there is no match
    """

    def pa(tokens, loc, string):
        for name, expected_value in attr.items():
            if name not in tokens:
                raise ParseException(
                    tokens.type, loc, string, f"is expecting {name} attribute"
                )
            if (
                expected_value != with_attribute.ANY_VALUE
                and tokens[name] != expected_value
            ):
                raise ParseException(
                    tokens.type,
                    loc,
                    string,
                    f"attribute '{name}' has value '{tokens[name]}', must be"
                    f" '{expected_value}'",
                )

    return pa


with_attribute.ANY_VALUE = object()


def with_class(classname, namespace=""):
    """Simplified version of `with_attribute` when
    matching on a div class - made difficult because ``class`` is
    a reserved word in Python.

    Example::

        html = '''
            <div>
            Some text
            <div class="grid">1 4 0 1 0</div>
            <div class="graph">1,3 2,3 1,1</div>
            <div>this &lt;div&gt; has no class</div>
            </div>

        '''
        div,div_end = makeHTMLTags("div")
        div_grid = div()/ withClass("grid")

        grid_expr = div_grid + SkipTo(div | div_end)("body")
        for grid_header in grid_expr.search_string(html):
            print(grid_header.body)

        div_any_type = div()/ withClass(with_attribute.ANY_VALUE)
        div_expr = div_any_type + SkipTo(div | div_end)("body")
        for div_header in div_expr.search_string(html):
            print(div_header.body)

    prints::

        1 4 0 1 0

        1 4 0 1 0
        1,3 2,3 1,1
    """
    classattr = "%s:class" % namespace if namespace else "class"
    return with_attribute(**{classattr: classname})


_indent_stack = [(1, None, None)]


def reset_stack():
    global _indent_stack
    _indent_stack = [(1, None, None)]


add_reset_action(reset_stack)


def indented_block(blockStatementExpr, indent=True):
    """Helper method for defining space-delimited indentation blocks,
    such as those used to define block statements in Python source code.

    Parameters:

     - blockStatementExpr - expression defining syntax of statement that
       is repeated within the indented block
     - indentStack - list created by caller to manage indentation stack
       (multiple statementWithIndentedBlock expressions within a single
       grammar should share a common indentStack)
     - indent - boolean indicating whether block must be indented beyond
       the current level; set to False for block of left-most
       statements (default= ``True``)

    A valid block must contain at least one ``blockStatement``.
    """
    PEER = Forward()
    DEDENT = Forward()

    def _reset_stack(t=None, i=None, s=None, c=None):
        old_col, old_peer, old_dedent = _indent_stack.pop()
        PEER << old_peer
        DEDENT << old_dedent

    def peer_stack(expected_col):
        def output(t, l, s):
            if l >= len(s):
                return
            cur_col = col(l, s)
            if cur_col != expected_col:
                if cur_col > expected_col:
                    raise ParseException(t.type, l, s, "illegal nesting")
                raise ParseException(t.type, l, s, "not a peer entry")

        return output

    def dedent_stack(expected_col):
        def output(t, l, s):
            if l >= len(s):
                return
            cur_col = col(l, s)
            if cur_col not in (i for i, _, _ in _indent_stack):
                raise ParseException(s, l, "not an unindent")
            if cur_col < _indent_stack[-1][0]:
                old_col, old_peer, old_dedent = _indent_stack.pop()
                PEER << old_peer
                DEDENT << old_dedent

        return output

    def indent_stack(t, l, s):
        cur_col = col(l, s)
        if cur_col > _indent_stack[-1][0]:
            PEER << Empty() / peer_stack(cur_col)
            DEDENT << Empty() / dedent_stack(cur_col)
            _indent_stack.append((cur_col, PEER, DEDENT))
        else:
            raise ParseException(t.type, l, s, "not a subentry")

    def nodent_stack(t, l, s):
        cur_col = col(l, s)
        if cur_col == _indent_stack[-1][0]:
            PEER << Empty() / peer_stack(cur_col)
            DEDENT << Empty() / dedent_stack(cur_col)
            _indent_stack.append((cur_col, PEER, DEDENT))
        else:
            raise ParseException(t.type, l, s, "not a subentry")

    ignore_list = whitespaces.CURRENT.ignore_list
    with Whitespace(whitespaces.CURRENT.white_chars) as e:
        e.add_ignore(*ignore_list)

        NL = OneOrMore(LineEnd().suppress())
        INDENT = Empty() / indent_stack
        NODENT = Empty() / nodent_stack

        if indent:
            sm_expr = Group(
                Optional(NL)
                + INDENT
                + OneOrMore(PEER + Group(blockStatementExpr) + Optional(NL))
                + DEDENT
            )
        else:
            sm_expr = Group(
                Optional(NL)
                + NODENT
                + OneOrMore(PEER + Group(blockStatementExpr) + Optional(NL))
                + DEDENT
            )
    return sm_expr.setFailAction(_reset_stack).set_parser_name("indented block")


anyOpenTag, anyCloseTag = makeHTMLTags(
    Word(alphas, alphanums + "_:").set_parser_name("any tag")
)
_htmlEntityMap = dict(zip("gt lt amp nbsp quot apos".split(), "><& \"'"))
commonHTMLEntity = Regex(
    "&(?P<entity>" + "|".join(_htmlEntityMap.keys()) + ");"
).set_parser_name("common HTML entity")


def replaceHTMLEntity(t):
    """Helper parser action to replace common HTML entities with their special characters"""
    return _htmlEntityMap.get(t.entity)


# it's easy to get these comment structures wrong - they're very common, so may as well make them available
cStyleComment = Combine(
    Regex(r"/\*(?:[^*]|\*(?!/))*") + "*/"
).set_parser_name("C style comment")

html_comment = Regex(r"<!--[\s\S]*?-->").set_parser_name("HTML comment")

with NO_WHITESPACE:
    restOfLine = Regex(r"[^\n]*").set_parser_name("rest of line")

    dblSlashComment = Regex(r"//(?:\\\n|[^\n])*").set_parser_name("// comment")

    cppStyleComment = Combine(
        Regex(r"/\*(?:[^*]|\*(?!/))*") + "*/" | dblSlashComment
    ).set_parser_name("C++ style comment")

    javaStyleComment = cppStyleComment

    pythonStyleComment = Regex(r"#[^\n]*").set_parser_name("Python style comment")

_commasepitem = Combine(OneOrMore(
    Word(printables, exclude=",") + Optional(Word(" \t") + ~Literal(",") + ~LineEnd())
)).set_parser_name("comma_item") / (lambda t: text(t).strip())
commaSeparatedList = delimited_list(Optional(
    quoted_string | _commasepitem, default=""
)).set_parser_name("commaSeparatedList")

convertToInteger = token_map(int)
convertToFloat = token_map(float)

integer = Word(nums).set_parser_name("integer") / convertToInteger

hex_integer = Word(hexnums).set_parser_name("hex integer") / token_map(int, 16)

signed_integer = Regex(r"[+-]?\d+").set_parser_name("signed integer") / convertToInteger

fraction = (
    signed_integer / convertToFloat + "/" + signed_integer / convertToFloat
).set_parser_name("fraction") / (lambda t: t[0] / t[2])

mixed_integer = (
    fraction | signed_integer + Optional(Optional("-").suppress() + fraction)
).set_parser_name("fraction or mixed integer-fraction") / sum

real = Regex(r"[+-]?(?:\d+\.\d*|\.\d+)").set_parser_name("real number") / convertToFloat

sci_real = (
    Regex(r"[+-]?(?:\d+(?:[eE][+-]?\d+)|(?:\d+\.\d*|\.\d+)(?:[eE][+-]?\d+)?)").set_parser_name("real number with scientific notation")
    / convertToFloat
)

number = (sci_real | real | signed_integer).streamline()

fnumber = (
    Regex(r"[+-]?\d+\.?\d*([eE][+-]?\d+)?").set_parser_name("fnumber") / convertToFloat
)

identifier = Word(alphas + "_", alphanums + "_").set_parser_name("identifier")

ipv4_address = Regex(
    r"(25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})(\.(25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})){3}"
).set_parser_name("IPv4 address")

_ipv6_part = Regex(r"[0-9a-fA-F]{1,4}").set_parser_name("hex_integer")
_full_ipv6_address = (
    _ipv6_part + (":" + _ipv6_part) * 7
).set_parser_name("full IPv6 address")
_short_ipv6_address = (
    Optional(_ipv6_part + (":" + _ipv6_part) * (0, 6))
    + "::"
    + Optional(_ipv6_part + (":" + _ipv6_part) * (0, 6))
).set_parser_name("short IPv6 address")
_short_ipv6_address.add_condition(lambda t: sum(
    1 for tt in t if _ipv6_part.matches(tt)
) < 8)
_mixed_ipv6_address = ("::ffff:" + ipv4_address).set_parser_name("mixed IPv6 address")
ipv6_address = Combine(
    (
        _full_ipv6_address | _mixed_ipv6_address | _short_ipv6_address
    ).set_parser_name("IPv6 address")
).set_parser_name("IPv6 address")
"IPv6 address (long, short, or mixed form)"

mac_address = (
    Regex(r"[0-9a-fA-F]{2}([:.-])[0-9a-fA-F]{2}(?:\1[0-9a-fA-F]{2}){4}").set_parser_name("MAC address")
)
"MAC address xx:xx:xx:xx:xx (may also have '-' or '.' delimiters)"


def convertToDate(fmt="%Y-%m-%d"):
    """
    Helper to create a parse action for converting parsed date string to Python datetime.date

    Params -
     - fmt - format to be passed to datetime.strptime (default= ``"%Y-%m-%d"``)

    Example::

        date_expr = iso8601_date.copy()
        date_expr/ convertToDate()
        print(date_expr.parse_string("1999-12-31"))

    prints::

        [datetime.date(1999, 12, 31)]
    """

    def cvt_fn(t, l, s):
        try:
            return datetime.strptime(s[t.start : t.end], fmt).date()
        except ValueError as ve:
            raise ParseException(t.type, l, s, str(ve))

    return cvt_fn


def convertToDatetime(fmt="%Y-%m-%dT%H:%M:%S.%f"):
    """Helper to create a parse action for converting parsed
    datetime string to Python datetime.datetime

    Params -
     - fmt - format to be passed to datetime.strptime (default= ``"%Y-%m-%dT%H:%M:%S.%f"``)

    Example::

        dt_expr = iso8601_datetime.copy()
        dt_expr/ convertToDatetime()
        print(dt_expr.parse_string("1999-12-31T23:59:59.999"))

    prints::

        [datetime.datetime(1999, 12, 31, 23, 59, 59, 999000)]
    """

    def cvt_fn(t, l, s):
        try:
            return datetime.strptime(s[t.start : t.end], fmt)
        except ValueError as ve:
            raise ParseException(t.type, l, s, str(ve))

    return cvt_fn


iso8601_date = (
    Regex(r"(?P<year>\d{4})(?:-(?P<month>\d\d)(?:-(?P<day>\d\d))?)?")
    .capture_groups()
    .set_parser_name("ISO8601 date")
)

iso8601_datetime = (
    Regex(
        r"(?P<year>\d{4})-(?P<month>\d\d)-(?P<day>\d\d)[T"
        r" ](?P<hour>\d\d):(?P<minute>\d\d)(:(?P<second>\d\d(\.\d*)?)?)?(?P<tz>Z|[+-]\d\d:?\d\d)?"
    )
    .capture_groups()
    .set_parser_name("ISO8601 datetime")
)

uuid = (
    Regex(r"[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}").set_parser_name("UUID")
)

_html_stripper = anyOpenTag.suppress() | anyCloseTag.suppress()


def stripHTMLTags(tokens, l, s):
    """Parse action to remove HTML tags from web page HTML source

    Example::

        # strip HTML links from normal text
        text = '<td>More info at the <a href="https://github.com/mo_parsing/mo_parsing/wiki">mo_parsing</a> wiki page</td>'
        td, td_end = makeHTMLTags("TD")
        table_text = td + SkipTo(td_end)/ stripHTMLTags)("body" + td_end
        print(table_text.parse_string(text).body)

    Prints::

        More info at the mo_parsing wiki page
    """
    return _html_stripper.transform_string(tokens[0])


def _strip(tok):
    return "".join(tok).strip()


_commasepitem = (
    Word(printables + " \t", exclude=",").set_parser_name("comma_item") / _strip
)
comma_separated_list = delimited_list(Optional(
    quoted_string | _commasepitem, default=""
)).set_parser_name("comma separated list")
