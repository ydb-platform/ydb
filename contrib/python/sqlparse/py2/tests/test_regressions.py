# -*- coding: utf-8 -*-

import pytest

import sqlparse
from sqlparse import sql, tokens as T
from sqlparse.compat import PY2


def test_issue9():
    # make sure where doesn't consume parenthesis
    p = sqlparse.parse('(where 1)')[0]
    assert isinstance(p, sql.Statement)
    assert len(p.tokens) == 1
    assert isinstance(p.tokens[0], sql.Parenthesis)
    prt = p.tokens[0]
    assert len(prt.tokens) == 3
    assert prt.tokens[0].ttype == T.Punctuation
    assert prt.tokens[-1].ttype == T.Punctuation


def test_issue13():
    parsed = sqlparse.parse(("select 'one';\n"
                             "select 'two\\'';\n"
                             "select 'three';"))
    assert len(parsed) == 3
    assert str(parsed[1]).strip() == "select 'two\\'';"


@pytest.mark.parametrize('s', ['--hello', '-- hello', '--hello\n',
                               '--', '--\n'])
def test_issue26(s):
    # parse stand-alone comments
    p = sqlparse.parse(s)[0]
    assert len(p.tokens) == 1
    assert p.tokens[0].ttype is T.Comment.Single


@pytest.mark.parametrize('value', ['create', 'CREATE'])
def test_issue34(value):
    t = sqlparse.parse("create")[0].token_first()
    assert t.match(T.Keyword.DDL, value) is True


def test_issue35():
    # missing space before LIMIT. Updated for #321
    sql = sqlparse.format("select * from foo where bar = 1 limit 1",
                          reindent=True)
    assert sql == "\n".join([
        "select *",
        "from foo",
        "where bar = 1",
        "limit 1"])


def test_issue38():
    sql = sqlparse.format("SELECT foo; -- comment", strip_comments=True)
    assert sql == "SELECT foo;"
    sql = sqlparse.format("/* foo */", strip_comments=True)
    assert sql == ""


def test_issue39():
    p = sqlparse.parse('select user.id from user')[0]
    assert len(p.tokens) == 7
    idt = p.tokens[2]
    assert idt.__class__ == sql.Identifier
    assert len(idt.tokens) == 3
    assert idt.tokens[0].match(T.Name, 'user') is True
    assert idt.tokens[1].match(T.Punctuation, '.') is True
    assert idt.tokens[2].match(T.Name, 'id') is True


def test_issue40():
    # make sure identifier lists in subselects are grouped
    p = sqlparse.parse(('SELECT id, name FROM '
                        '(SELECT id, name FROM bar) as foo'))[0]
    assert len(p.tokens) == 7
    assert p.tokens[2].__class__ == sql.IdentifierList
    assert p.tokens[-1].__class__ == sql.Identifier
    assert p.tokens[-1].get_name() == 'foo'
    sp = p.tokens[-1].tokens[0]
    assert sp.tokens[3].__class__ == sql.IdentifierList
    # make sure that formatting works as expected
    s = sqlparse.format('SELECT id ==  name FROM '
                        '(SELECT id, name FROM bar)', reindent=True)
    assert s == '\n'.join([
        'SELECT id == name',
        'FROM',
        '  (SELECT id,',
        '          name',
        '   FROM bar)'])

    s = sqlparse.format('SELECT id ==  name FROM '
                        '(SELECT id, name FROM bar) as foo', reindent=True)
    assert s == '\n'.join([
        'SELECT id == name',
        'FROM',
        '  (SELECT id,',
        '          name',
        '   FROM bar) as foo'])


@pytest.mark.parametrize('s', ['select x.y::text as z from foo',
                               'select x.y::text as "z" from foo',
                               'select x."y"::text as z from foo',
                               'select x."y"::text as "z" from foo',
                               'select "x".y::text as z from foo',
                               'select "x".y::text as "z" from foo',
                               'select "x"."y"::text as z from foo',
                               'select "x"."y"::text as "z" from foo'])
@pytest.mark.parametrize('func_name, result', [('get_name', 'z'),
                                               ('get_real_name', 'y'),
                                               ('get_parent_name', 'x'),
                                               ('get_alias', 'z'),
                                               ('get_typecast', 'text')])
def test_issue78(s, func_name, result):
    # the bug author provided this nice examples, let's use them!
    p = sqlparse.parse(s)[0]
    i = p.tokens[2]
    assert isinstance(i, sql.Identifier)

    func = getattr(i, func_name)
    assert func() == result


def test_issue83():
    sql = """   CREATE OR REPLACE FUNCTION func_a(text)
                  RETURNS boolean  LANGUAGE plpgsql STRICT IMMUTABLE AS
                $_$
                BEGIN
                 ...
                END;
                $_$;

                CREATE OR REPLACE FUNCTION func_b(text)
                  RETURNS boolean  LANGUAGE plpgsql STRICT IMMUTABLE AS
                $_$
                BEGIN
                 ...
                END;
                $_$;

                ALTER TABLE..... ;"""
    t = sqlparse.split(sql)
    assert len(t) == 3


def test_comment_encoding_when_reindent():
    # There was an UnicodeEncodeError in the reindent filter that
    # casted every comment followed by a keyword to str.
    sql = u'select foo -- Comment containing Ümläuts\nfrom bar'
    formatted = sqlparse.format(sql, reindent=True)
    assert formatted == sql


def test_parse_sql_with_binary():
    # See https://github.com/andialbrecht/sqlparse/pull/88
    # digest = '|ËêplL4¡høN{'
    digest = '\x82|\xcb\x0e\xea\x8aplL4\xa1h\x91\xf8N{'
    sql = "select * from foo where bar = '{0}'".format(digest)
    formatted = sqlparse.format(sql, reindent=True)
    tformatted = "select *\nfrom foo\nwhere bar = '{0}'".format(digest)
    if PY2:
        tformatted = tformatted.decode('unicode-escape')
    assert formatted == tformatted


def test_dont_alias_keywords():
    # The _group_left_right function had a bug where the check for the
    # left side wasn't handled correctly. In one case this resulted in
    # a keyword turning into an identifier.
    p = sqlparse.parse('FROM AS foo')[0]
    assert len(p.tokens) == 5
    assert p.tokens[0].ttype is T.Keyword
    assert p.tokens[2].ttype is T.Keyword


def test_format_accepts_encoding(load_file):
    # issue20
    sql = load_file('test_cp1251.sql', 'cp1251')
    formatted = sqlparse.format(sql, reindent=True, encoding='cp1251')
    tformatted = u'insert into foo\nvalues (1); -- Песня про надежду'

    assert formatted == tformatted


def test_stream(get_stream):
    with get_stream("stream.sql") as stream:
        p = sqlparse.parse(stream)[0]
        assert p.get_type() == 'INSERT'


def test_issue90():
    sql = ('UPDATE "gallery_photo" SET "owner_id" = 4018, "deleted_at" = NULL,'
           ' "width" = NULL, "height" = NULL, "rating_votes" = 0,'
           ' "rating_score" = 0, "thumbnail_width" = NULL,'
           ' "thumbnail_height" = NULL, "price" = 1, "description" = NULL')
    formatted = sqlparse.format(sql, reindent=True)
    tformatted = '\n'.join([
        'UPDATE "gallery_photo"',
        'SET "owner_id" = 4018,',
        '    "deleted_at" = NULL,',
        '    "width" = NULL,',
        '    "height" = NULL,',
        '    "rating_votes" = 0,',
        '    "rating_score" = 0,',
        '    "thumbnail_width" = NULL,',
        '    "thumbnail_height" = NULL,',
        '    "price" = 1,',
        '    "description" = NULL'])
    assert formatted == tformatted


def test_except_formatting():
    sql = 'SELECT 1 FROM foo WHERE 2 = 3 EXCEPT SELECT 2 FROM bar WHERE 1 = 2'
    formatted = sqlparse.format(sql, reindent=True)
    tformatted = '\n'.join([
        'SELECT 1',
        'FROM foo',
        'WHERE 2 = 3',
        'EXCEPT',
        'SELECT 2',
        'FROM bar',
        'WHERE 1 = 2'])
    assert formatted == tformatted


def test_null_with_as():
    sql = 'SELECT NULL AS c1, NULL AS c2 FROM t1'
    formatted = sqlparse.format(sql, reindent=True)
    tformatted = '\n'.join([
        'SELECT NULL AS c1,',
        '       NULL AS c2',
        'FROM t1'])
    assert formatted == tformatted


def test_issue190_open_file(filepath):
    path = filepath('stream.sql')
    with open(path) as stream:
        p = sqlparse.parse(stream)[0]
        assert p.get_type() == 'INSERT'


def test_issue193_splitting_function():
    sql = """   CREATE FUNCTION a(x VARCHAR(20)) RETURNS VARCHAR(20)
                BEGIN
                 DECLARE y VARCHAR(20);
                 RETURN x;
                END;
                SELECT * FROM a.b;"""
    statements = sqlparse.split(sql)
    assert len(statements) == 2


def test_issue194_splitting_function():
    sql = """   CREATE FUNCTION a(x VARCHAR(20)) RETURNS VARCHAR(20)
                BEGIN
                 DECLARE y VARCHAR(20);
                 IF (1 = 1) THEN
                 SET x = y;
                 END IF;
                 RETURN x;
                END;
                SELECT * FROM a.b;"""
    statements = sqlparse.split(sql)
    assert len(statements) == 2


def test_issue186_get_type():
    sql = "-- comment\ninsert into foo"
    p = sqlparse.parse(sql)[0]
    assert p.get_type() == 'INSERT'


def test_issue212_py2unicode():
    t1 = sql.Token(T.String, u'schöner ')
    t2 = sql.Token(T.String, 'bug')
    token_list = sql.TokenList([t1, t2])
    assert str(token_list) == 'schöner bug'


def test_issue213_leadingws():
    sql = " select * from foo"
    assert sqlparse.format(sql, strip_whitespace=True) == "select * from foo"


def test_issue227_gettype_cte():
    select_stmt = sqlparse.parse('SELECT 1, 2, 3 FROM foo;')
    assert select_stmt[0].get_type() == 'SELECT'
    with_stmt = sqlparse.parse('WITH foo AS (SELECT 1, 2, 3)'
                               'SELECT * FROM foo;')
    assert with_stmt[0].get_type() == 'SELECT'
    with2_stmt = sqlparse.parse("""
        WITH foo AS (SELECT 1 AS abc, 2 AS def),
             bar AS (SELECT * FROM something WHERE x > 1)
        INSERT INTO elsewhere SELECT * FROM foo JOIN bar;""")
    assert with2_stmt[0].get_type() == 'INSERT'


def test_issue207_runaway_format():
    sql = 'select 1 from (select 1 as one, 2 as two, 3 from dual) t0'
    p = sqlparse.format(sql, reindent=True)
    assert p == '\n'.join([
        "select 1",
        "from",
        "  (select 1 as one,",
        "          2 as two,",
        "          3",
        "   from dual) t0"])


def test_token_next_doesnt_ignore_skip_cm():
    sql = '--comment\nselect 1'
    tok = sqlparse.parse(sql)[0].token_next(-1, skip_cm=True)[1]
    assert tok.value == 'select'


@pytest.mark.parametrize('s', [
    'SELECT x AS',
    'AS'
])
def test_issue284_as_grouping(s):
    p = sqlparse.parse(s)[0]
    assert s == str(p)


def test_issue315_utf8_by_default():
    # Make sure the lexer can handle utf-8 string by default correctly
    # digest = '齐天大圣.カラフルな雲.사랑해요'
    # The digest contains Chinese, Japanese and Korean characters
    # All in 'utf-8' encoding.
    digest = (
        '\xe9\xbd\x90\xe5\xa4\xa9\xe5\xa4\xa7\xe5\x9c\xa3.'
        '\xe3\x82\xab\xe3\x83\xa9\xe3\x83\x95\xe3\x83\xab\xe3\x81\xaa\xe9'
        '\x9b\xb2.'
        '\xec\x82\xac\xeb\x9e\x91\xed\x95\xb4\xec\x9a\x94'
    )
    sql = "select * from foo where bar = '{0}'".format(digest)
    formatted = sqlparse.format(sql, reindent=True)
    tformatted = "select *\nfrom foo\nwhere bar = '{0}'".format(digest)
    if PY2:
        tformatted = tformatted.decode('utf-8')
    assert formatted == tformatted


def test_issue322_concurrently_is_keyword():
    s = 'CREATE INDEX CONCURRENTLY myindex ON mytable(col1);'
    p = sqlparse.parse(s)[0]

    assert len(p.tokens) == 12
    assert p.tokens[0].ttype is T.Keyword.DDL  # CREATE
    assert p.tokens[2].ttype is T.Keyword      # INDEX
    assert p.tokens[4].ttype is T.Keyword      # CONCURRENTLY
    assert p.tokens[4].value == 'CONCURRENTLY'
    assert isinstance(p.tokens[6], sql.Identifier)
    assert p.tokens[6].value == 'myindex'


@pytest.mark.parametrize('s', [
    'SELECT @min_price:=MIN(price), @max_price:=MAX(price) FROM shop;',
    'SELECT @min_price:=MIN(price), @max_price:=MAX(price) FROM shop',

])
def test_issue359_index_error_assignments(s):
    sqlparse.parse(s)
    sqlparse.format(s, strip_comments=True)


def test_issue469_copy_as_psql_command():
    formatted = sqlparse.format(
        '\\copy select * from foo',
        keyword_case='upper', identifier_case='capitalize')
    assert formatted == '\\copy SELECT * FROM Foo'


@pytest.mark.xfail(reason='Needs to be fixed')
def test_issue484_comments_and_newlines():
    formatted = sqlparse.format('\n'.join([
        'Create table myTable',
        '(',
        '  myId TINYINT NOT NULL, --my special comment',
        '  myName VARCHAR2(100) NOT NULL',
        ')']),
        strip_comments=True)
    assert formatted == ('\n'.join([
        'Create table myTable',
        '(',
        '  myId TINYINT NOT NULL,',
        '  myName VARCHAR2(100) NOT NULL',
        ')']))


def test_issue485_split_multi():
    p_sql = '''CREATE OR REPLACE RULE ruled_tab_2rules AS ON INSERT
TO public.ruled_tab
DO instead (
select 1;
select 2;
);'''
    assert len(sqlparse.split(p_sql)) == 1


def test_issue489_tzcasts():
    p = sqlparse.parse('select bar at time zone \'UTC\' as foo')[0]
    assert p.tokens[-1].has_alias() is True
    assert p.tokens[-1].get_alias() == 'foo'
