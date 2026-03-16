"""Tests sqlparse.parse()."""
from io import StringIO

import pytest

import sqlparse
from sqlparse import sql, tokens as T, keywords
from sqlparse.lexer import Lexer


def test_parse_tokenize():
    s = 'select * from foo;'
    stmts = sqlparse.parse(s)
    assert len(stmts) == 1
    assert str(stmts[0]) == s


def test_parse_multistatement():
    sql1 = 'select * from foo;'
    sql2 = 'select * from bar;'
    stmts = sqlparse.parse(sql1 + sql2)
    assert len(stmts) == 2
    assert str(stmts[0]) == sql1
    assert str(stmts[1]) == sql2


@pytest.mark.parametrize('s', ['select\n*from foo;',
                               'select\r\n*from foo',
                               'select\r*from foo',
                               'select\r\n*from foo\n'])
def test_parse_newlines(s):
    p = sqlparse.parse(s)[0]
    assert str(p) == s


def test_parse_within():
    s = 'foo(col1, col2)'
    p = sqlparse.parse(s)[0]
    col1 = p.tokens[0].tokens[1].tokens[1].tokens[0]
    assert col1.within(sql.Function)


def test_parse_child_of():
    s = '(col1, col2)'
    p = sqlparse.parse(s)[0]
    assert p.tokens[0].tokens[1].is_child_of(p.tokens[0])
    s = 'select foo'
    p = sqlparse.parse(s)[0]
    assert not p.tokens[2].is_child_of(p.tokens[0])
    assert p.tokens[2].is_child_of(p)


def test_parse_has_ancestor():
    s = 'foo or (bar, baz)'
    p = sqlparse.parse(s)[0]
    baz = p.tokens[-1].tokens[1].tokens[-1]
    assert baz.has_ancestor(p.tokens[-1].tokens[1])
    assert baz.has_ancestor(p.tokens[-1])
    assert baz.has_ancestor(p)


@pytest.mark.parametrize('s', ['.5', '.51', '1.5', '12.5'])
def test_parse_float(s):
    t = sqlparse.parse(s)[0].tokens
    assert len(t) == 1
    assert t[0].ttype is sqlparse.tokens.Number.Float


@pytest.mark.parametrize('s, holder', [
    ('select * from foo where user = ?', '?'),
    ('select * from foo where user = :1', ':1'),
    ('select * from foo where user = :name', ':name'),
    ('select * from foo where user = %s', '%s'),
    ('select * from foo where user = $a', '$a')])
def test_parse_placeholder(s, holder):
    t = sqlparse.parse(s)[0].tokens[-1].tokens
    assert t[-1].ttype is sqlparse.tokens.Name.Placeholder
    assert t[-1].value == holder


def test_parse_modulo_not_placeholder():
    tokens = list(sqlparse.lexer.tokenize('x %3'))
    assert tokens[2][0] == sqlparse.tokens.Operator


def test_parse_access_symbol():
    # see issue27
    t = sqlparse.parse('select a.[foo bar] as foo')[0].tokens
    assert isinstance(t[-1], sql.Identifier)
    assert t[-1].get_name() == 'foo'
    assert t[-1].get_real_name() == '[foo bar]'
    assert t[-1].get_parent_name() == 'a'


def test_parse_square_brackets_notation_isnt_too_greedy():
    # see issue153
    t = sqlparse.parse('[foo], [bar]')[0].tokens
    assert isinstance(t[0], sql.IdentifierList)
    assert len(t[0].tokens) == 4
    assert t[0].tokens[0].get_real_name() == '[foo]'
    assert t[0].tokens[-1].get_real_name() == '[bar]'


def test_parse_square_brackets_notation_isnt_too_greedy2():
    # see issue583
    t = sqlparse.parse('[(foo[i])]')[0].tokens
    assert isinstance(t[0], sql.SquareBrackets)  # not Identifier!


def test_parse_keyword_like_identifier():
    # see issue47
    t = sqlparse.parse('foo.key')[0].tokens
    assert len(t) == 1
    assert isinstance(t[0], sql.Identifier)


def test_parse_function_parameter():
    # see issue94
    t = sqlparse.parse('abs(some_col)')[0].tokens[0].get_parameters()
    assert len(t) == 1
    assert isinstance(t[0], sql.Identifier)


def test_parse_function_param_single_literal():
    t = sqlparse.parse('foo(5)')[0].tokens[0].get_parameters()
    assert len(t) == 1
    assert t[0].ttype is T.Number.Integer


def test_parse_nested_function():
    t = sqlparse.parse('foo(bar(5))')[0].tokens[0].get_parameters()
    assert len(t) == 1
    assert type(t[0]) is sql.Function


def test_parse_casted_params():
    t = sqlparse.parse("foo(DATE '2023-11-14', TIMESTAMP '2023-11-15')")[0].tokens[0].get_parameters()
    assert len(t) == 2


def test_parse_div_operator():
    p = sqlparse.parse('col1 DIV 5 AS div_col1')[0].tokens
    assert p[0].tokens[0].tokens[2].ttype is T.Operator
    assert p[0].get_alias() == 'div_col1'


def test_quoted_identifier():
    t = sqlparse.parse('select x.y as "z" from foo')[0].tokens
    assert isinstance(t[2], sql.Identifier)
    assert t[2].get_name() == 'z'
    assert t[2].get_real_name() == 'y'


@pytest.mark.parametrize('name', [
    'foo', '_foo',  # issue175
    '1_data',  # valid MySQL table name, see issue337
    '業者名稱',  # valid at least for SQLite3, see issue641
])
def test_valid_identifier_names(name):
    t = sqlparse.parse(name)[0].tokens
    assert isinstance(t[0], sql.Identifier)
    assert t[0].get_name() == name


def test_psql_quotation_marks():
    # issue83

    # regression: make sure plain $$ work
    t = sqlparse.split("""
    CREATE OR REPLACE FUNCTION testfunc1(integer) RETURNS integer AS $$
          ....
    $$ LANGUAGE plpgsql;
    CREATE OR REPLACE FUNCTION testfunc2(integer) RETURNS integer AS $$
          ....
    $$ LANGUAGE plpgsql;""")
    assert len(t) == 2

    # make sure $SOMETHING$ works too
    t = sqlparse.split("""
    CREATE OR REPLACE FUNCTION testfunc1(integer) RETURNS integer AS $PROC_1$
          ....
    $PROC_1$ LANGUAGE plpgsql;
    CREATE OR REPLACE FUNCTION testfunc2(integer) RETURNS integer AS $PROC_2$
          ....
    $PROC_2$ LANGUAGE plpgsql;""")
    assert len(t) == 2

    # operators are valid infront of dollar quoted strings
    t = sqlparse.split("""UPDATE SET foo =$$bar;SELECT bar$$""")
    assert len(t) == 1
    
    # identifiers must be separated by whitespace
    t = sqlparse.split("""UPDATE SET foo TO$$bar;SELECT bar$$""")
    assert len(t) == 2


def test_double_precision_is_builtin():
    s = 'DOUBLE PRECISION'
    t = sqlparse.parse(s)[0].tokens
    assert len(t) == 1
    assert t[0].ttype == sqlparse.tokens.Name.Builtin
    assert t[0].value == 'DOUBLE PRECISION'


@pytest.mark.parametrize('ph', ['?', ':1', ':foo', '%s', '%(foo)s'])
def test_placeholder(ph):
    p = sqlparse.parse(ph)[0].tokens
    assert len(p) == 1
    assert p[0].ttype is T.Name.Placeholder


@pytest.mark.parametrize('num, expected', [
    ('6.67428E-8', T.Number.Float),
    ('1.988e33', T.Number.Float),
    ('1e-12', T.Number.Float),
    ('e1', None),
])
def test_scientific_numbers(num, expected):
    p = sqlparse.parse(num)[0].tokens
    assert len(p) == 1
    assert p[0].ttype is expected


def test_single_quotes_are_strings():
    p = sqlparse.parse("'foo'")[0].tokens
    assert len(p) == 1
    assert p[0].ttype is T.String.Single


def test_double_quotes_are_identifiers():
    p = sqlparse.parse('"foo"')[0].tokens
    assert len(p) == 1
    assert isinstance(p[0], sql.Identifier)


def test_single_quotes_with_linebreaks():
    # issue118
    p = sqlparse.parse("'f\nf'")[0].tokens
    assert len(p) == 1
    assert p[0].ttype is T.String.Single


def test_sqlite_identifiers():
    # Make sure we still parse sqlite style escapes
    p = sqlparse.parse('[col1],[col2]')[0].tokens
    id_names = [id_.get_name() for id_ in p[0].get_identifiers()]
    assert len(p) == 1
    assert isinstance(p[0], sql.IdentifierList)
    assert id_names == ['[col1]', '[col2]']

    p = sqlparse.parse('[col1]+[col2]')[0]
    types = [tok.ttype for tok in p.flatten()]
    assert types == [T.Name, T.Operator, T.Name]


def test_simple_1d_array_index():
    p = sqlparse.parse('col[1]')[0].tokens
    assert len(p) == 1
    assert p[0].get_name() == 'col'
    indices = list(p[0].get_array_indices())
    assert len(indices) == 1  # 1-dimensional index
    assert len(indices[0]) == 1  # index is single token
    assert indices[0][0].value == '1'


def test_2d_array_index():
    p = sqlparse.parse('col[x][(y+1)*2]')[0].tokens
    assert len(p) == 1
    assert p[0].get_name() == 'col'
    assert len(list(p[0].get_array_indices())) == 2  # 2-dimensional index


def test_array_index_function_result():
    p = sqlparse.parse('somefunc()[1]')[0].tokens
    assert len(p) == 1
    assert len(list(p[0].get_array_indices())) == 1


def test_schema_qualified_array_index():
    p = sqlparse.parse('schem.col[1]')[0].tokens
    assert len(p) == 1
    assert p[0].get_parent_name() == 'schem'
    assert p[0].get_name() == 'col'
    assert list(p[0].get_array_indices())[0][0].value == '1'


def test_aliased_array_index():
    p = sqlparse.parse('col[1] x')[0].tokens
    assert len(p) == 1
    assert p[0].get_alias() == 'x'
    assert p[0].get_real_name() == 'col'
    assert list(p[0].get_array_indices())[0][0].value == '1'


def test_array_literal():
    # See issue #176
    p = sqlparse.parse('ARRAY[%s, %s]')[0]
    assert len(p.tokens) == 2
    assert len(list(p.flatten())) == 7


def test_typed_array_definition():
    # array indices aren't grouped with built-ins, but make sure we can extract
    # identifier names
    p = sqlparse.parse('x int, y int[], z int')[0]
    names = [x.get_name() for x in p.get_sublists()
             if isinstance(x, sql.Identifier)]
    assert names == ['x', 'y', 'z']


@pytest.mark.parametrize('s', ['select 1 -- foo', 'select 1 # foo'])
def test_single_line_comments(s):
    # see issue178
    p = sqlparse.parse(s)[0]
    assert len(p.tokens) == 5
    assert p.tokens[-1].ttype == T.Comment.Single


@pytest.mark.parametrize('s', ['foo', '@foo', '#foo', '##foo'])
def test_names_and_special_names(s):
    # see issue192
    p = sqlparse.parse(s)[0]
    assert len(p.tokens) == 1
    assert isinstance(p.tokens[0], sql.Identifier)


def test_get_token_at_offset():
    p = sqlparse.parse('select * from dual')[0]
    #                   0123456789
    assert p.get_token_at_offset(0) == p.tokens[0]
    assert p.get_token_at_offset(1) == p.tokens[0]
    assert p.get_token_at_offset(6) == p.tokens[1]
    assert p.get_token_at_offset(7) == p.tokens[2]
    assert p.get_token_at_offset(8) == p.tokens[3]
    assert p.get_token_at_offset(9) == p.tokens[4]
    assert p.get_token_at_offset(10) == p.tokens[4]


def test_pprint():
    p = sqlparse.parse('select a0, b0, c0, d0, e0 from '
                       '(select * from dual) q0 where 1=1 and 2=2')[0]
    output = StringIO()

    p._pprint_tree(f=output)
    pprint = '\n'.join([
        "|- 0 DML 'select'",
        "|- 1 Whitespace ' '",
        "|- 2 IdentifierList 'a0, b0...'",
        "|  |- 0 Identifier 'a0'",
        "|  |  `- 0 Name 'a0'",
        "|  |- 1 Punctuation ','",
        "|  |- 2 Whitespace ' '",
        "|  |- 3 Identifier 'b0'",
        "|  |  `- 0 Name 'b0'",
        "|  |- 4 Punctuation ','",
        "|  |- 5 Whitespace ' '",
        "|  |- 6 Identifier 'c0'",
        "|  |  `- 0 Name 'c0'",
        "|  |- 7 Punctuation ','",
        "|  |- 8 Whitespace ' '",
        "|  |- 9 Identifier 'd0'",
        "|  |  `- 0 Name 'd0'",
        "|  |- 10 Punctuation ','",
        "|  |- 11 Whitespace ' '",
        "|  `- 12 Identifier 'e0'",
        "|     `- 0 Name 'e0'",
        "|- 3 Whitespace ' '",
        "|- 4 Keyword 'from'",
        "|- 5 Whitespace ' '",
        "|- 6 Identifier '(selec...'",
        "|  |- 0 Parenthesis '(selec...'",
        "|  |  |- 0 Punctuation '('",
        "|  |  |- 1 DML 'select'",
        "|  |  |- 2 Whitespace ' '",
        "|  |  |- 3 Wildcard '*'",
        "|  |  |- 4 Whitespace ' '",
        "|  |  |- 5 Keyword 'from'",
        "|  |  |- 6 Whitespace ' '",
        "|  |  |- 7 Identifier 'dual'",
        "|  |  |  `- 0 Name 'dual'",
        "|  |  `- 8 Punctuation ')'",
        "|  |- 1 Whitespace ' '",
        "|  `- 2 Identifier 'q0'",
        "|     `- 0 Name 'q0'",
        "|- 7 Whitespace ' '",
        "`- 8 Where 'where ...'",
        "   |- 0 Keyword 'where'",
        "   |- 1 Whitespace ' '",
        "   |- 2 Comparison '1=1'",
        "   |  |- 0 Integer '1'",
        "   |  |- 1 Comparison '='",
        "   |  `- 2 Integer '1'",
        "   |- 3 Whitespace ' '",
        "   |- 4 Keyword 'and'",
        "   |- 5 Whitespace ' '",
        "   `- 6 Comparison '2=2'",
        "      |- 0 Integer '2'",
        "      |- 1 Comparison '='",
        "      `- 2 Integer '2'",
        ""])
    assert output.getvalue() == pprint


def test_wildcard_multiplication():
    p = sqlparse.parse('select * from dual')[0]
    assert p.tokens[2].ttype == T.Wildcard

    p = sqlparse.parse('select a0.* from dual a0')[0]
    assert p.tokens[2][2].ttype == T.Wildcard

    p = sqlparse.parse('select 1 * 2 from dual')[0]
    assert p.tokens[2][2].ttype == T.Operator


def test_stmt_tokens_parents():
    # see issue 226
    s = "CREATE TABLE test();"
    stmt = sqlparse.parse(s)[0]
    for token in stmt.tokens:
        assert token.has_ancestor(stmt)


@pytest.mark.parametrize('sql, is_literal', [
    ('$$foo$$', True),
    ('$_$foo$_$', True),
    ('$token$ foo $token$', True),
    # don't parse inner tokens
    ('$_$ foo $token$bar$token$ baz$_$', True),
    ('$A$ foo $B$', False)  # tokens don't match
])
def test_dbldollar_as_literal(sql, is_literal):
    # see issue 277
    p = sqlparse.parse(sql)[0]
    if is_literal:
        assert len(p.tokens) == 1
        assert p.tokens[0].ttype == T.Literal
    else:
        for token in p.tokens:
            assert token.ttype != T.Literal


def test_non_ascii():
    _test_non_ascii = "insert into test (id, name) values (1, 'тест');"

    s = _test_non_ascii
    stmts = sqlparse.parse(s)
    assert len(stmts) == 1
    statement = stmts[0]
    assert str(statement) == s
    assert statement._pprint_tree() is None

    s = _test_non_ascii.encode('utf-8')
    stmts = sqlparse.parse(s, 'utf-8')
    assert len(stmts) == 1
    statement = stmts[0]
    assert str(statement) == _test_non_ascii
    assert statement._pprint_tree() is None


def test_get_real_name():
    # issue 369
    s = "update a t set t.b=1"
    stmts = sqlparse.parse(s)
    assert len(stmts) == 1
    assert 'a' == stmts[0].tokens[2].get_real_name()
    assert 't' == stmts[0].tokens[2].get_alias()


def test_from_subquery():
    # issue 446
    s = 'from(select 1)'
    stmts = sqlparse.parse(s)
    assert len(stmts) == 1
    assert len(stmts[0].tokens) == 2
    assert stmts[0].tokens[0].value == 'from'
    assert stmts[0].tokens[0].ttype == T.Keyword

    s = 'from (select 1)'
    stmts = sqlparse.parse(s)
    assert len(stmts) == 1
    assert len(stmts[0].tokens) == 3
    assert stmts[0].tokens[0].value == 'from'
    assert stmts[0].tokens[0].ttype == T.Keyword
    assert stmts[0].tokens[1].ttype == T.Whitespace


def test_parenthesis():
    tokens = sqlparse.parse("(\n\n1\n\n)")[0].tokens[0].tokens
    assert list(map(lambda t: t.ttype, tokens)) == [T.Punctuation,
                                                    T.Newline,
                                                    T.Newline,
                                                    T.Number.Integer,
                                                    T.Newline,
                                                    T.Newline,
                                                    T.Punctuation]
    tokens = sqlparse.parse("(\n\n 1 \n\n)")[0].tokens[0].tokens
    assert list(map(lambda t: t.ttype, tokens)) == [T.Punctuation,
                                                    T.Newline,
                                                    T.Newline,
                                                    T.Whitespace,
                                                    T.Number.Integer,
                                                    T.Whitespace,
                                                    T.Newline,
                                                    T.Newline,
                                                    T.Punctuation]


def test_configurable_keywords():
    sql = """select * from foo BACON SPAM EGGS;"""
    tokens = sqlparse.parse(sql)[0]

    assert list(
        (t.ttype, t.value)
        for t in tokens
        if t.ttype not in sqlparse.tokens.Whitespace
    ) == [
        (sqlparse.tokens.Keyword.DML, "select"),
        (sqlparse.tokens.Wildcard, "*"),
        (sqlparse.tokens.Keyword, "from"),
        (None, "foo BACON"),
        (None, "SPAM EGGS"),
        (sqlparse.tokens.Punctuation, ";"),
    ]

    Lexer.get_default_instance().add_keywords(
        {
            "BACON": sqlparse.tokens.Name.Builtin,
            "SPAM": sqlparse.tokens.Keyword,
            "EGGS": sqlparse.tokens.Keyword,
        }
    )

    tokens = sqlparse.parse(sql)[0]

    # reset the syntax for later tests.
    Lexer.get_default_instance().default_initialization()

    assert list(
        (t.ttype, t.value)
        for t in tokens
        if t.ttype not in sqlparse.tokens.Whitespace
    ) == [
        (sqlparse.tokens.Keyword.DML, "select"),
        (sqlparse.tokens.Wildcard, "*"),
        (sqlparse.tokens.Keyword, "from"),
        (None, "foo"),
        (sqlparse.tokens.Name.Builtin, "BACON"),
        (sqlparse.tokens.Keyword, "SPAM"),
        (sqlparse.tokens.Keyword, "EGGS"),
        (sqlparse.tokens.Punctuation, ";"),
    ]


def test_configurable_regex():
    lex = Lexer.get_default_instance()
    lex.clear()

    my_regex = (r"ZORDER\s+BY\b", sqlparse.tokens.Keyword)

    lex.set_SQL_REGEX(
        keywords.SQL_REGEX[:38]
        + [my_regex]
        + keywords.SQL_REGEX[38:]
    )
    lex.add_keywords(keywords.KEYWORDS_COMMON)
    lex.add_keywords(keywords.KEYWORDS_ORACLE)
    lex.add_keywords(keywords.KEYWORDS_PLPGSQL)
    lex.add_keywords(keywords.KEYWORDS_HQL)
    lex.add_keywords(keywords.KEYWORDS_MSACCESS)
    lex.add_keywords(keywords.KEYWORDS)

    tokens = sqlparse.parse("select * from foo zorder by bar;")[0]

    # reset the syntax for later tests.
    Lexer.get_default_instance().default_initialization()

    assert list(
        (t.ttype, t.value)
        for t in tokens
        if t.ttype not in sqlparse.tokens.Whitespace
    )[4] == (sqlparse.tokens.Keyword, "zorder by")


@pytest.mark.parametrize('sql', [
    '->', '->>', '#>', '#>>',
    '@>', '<@',
    # leaving ? out for now, they're somehow ambiguous as placeholders
    # '?', '?|', '?&',
    '||', '-', '#-'
])
def test_json_operators(sql):
    p = sqlparse.parse(sql)
    assert len(p) == 1
    assert len(p[0].tokens) == 1
    assert p[0].tokens[0].ttype == sqlparse.tokens.Operator
