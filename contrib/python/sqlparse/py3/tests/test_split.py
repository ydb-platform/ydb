# Tests splitting functions.

import types
from io import StringIO

import pytest

import sqlparse


def test_split_semicolon():
    sql1 = 'select * from foo;'
    sql2 = "select * from foo where bar = 'foo;bar';"
    stmts = sqlparse.parse(''.join([sql1, sql2]))
    assert len(stmts) == 2
    assert str(stmts[0]) == sql1
    assert str(stmts[1]) == sql2


def test_split_backslash():
    stmts = sqlparse.parse("select '\'; select '\'';")
    assert len(stmts) == 2


@pytest.mark.parametrize('fn', ['function.sql',
                                'function_psql.sql',
                                'function_psql2.sql',
                                'function_psql3.sql',
                                'function_psql4.sql'])
def test_split_create_function(load_file, fn):
    sql = load_file(fn)
    stmts = sqlparse.parse(sql)
    assert len(stmts) == 1
    assert str(stmts[0]) == sql


def test_split_dashcomments(load_file):
    sql = load_file('dashcomment.sql')
    stmts = sqlparse.parse(sql)
    assert len(stmts) == 3
    assert ''.join(str(q) for q in stmts) == sql


@pytest.mark.parametrize('s', ['select foo; -- comment\n',
                               'select foo; -- comment\r',
                               'select foo; -- comment\r\n',
                               'select foo; -- comment'])
def test_split_dashcomments_eol(s):
    stmts = sqlparse.parse(s)
    assert len(stmts) == 1


def test_split_begintag(load_file):
    sql = load_file('begintag.sql')
    stmts = sqlparse.parse(sql)
    assert len(stmts) == 3
    assert ''.join(str(q) for q in stmts) == sql


def test_split_begintag_2(load_file):
    sql = load_file('begintag_2.sql')
    stmts = sqlparse.parse(sql)
    assert len(stmts) == 1
    assert ''.join(str(q) for q in stmts) == sql


def test_split_dropif():
    sql = 'DROP TABLE IF EXISTS FOO;\n\nSELECT * FROM BAR;'
    stmts = sqlparse.parse(sql)
    assert len(stmts) == 2
    assert ''.join(str(q) for q in stmts) == sql


def test_split_comment_with_umlaut():
    sql = ('select * from foo;\n'
           '-- Testing an umlaut: ä\n'
           'select * from bar;')
    stmts = sqlparse.parse(sql)
    assert len(stmts) == 2
    assert ''.join(str(q) for q in stmts) == sql


def test_split_comment_end_of_line():
    sql = ('select * from foo; -- foo\n'
           'select * from bar;')
    stmts = sqlparse.parse(sql)
    assert len(stmts) == 2
    assert ''.join(str(q) for q in stmts) == sql
    # make sure the comment belongs to first query
    assert str(stmts[0]) == 'select * from foo; -- foo\n'


def test_split_casewhen():
    sql = ("SELECT case when val = 1 then 2 else null end as foo;\n"
           "comment on table actor is 'The actor table.';")
    stmts = sqlparse.split(sql)
    assert len(stmts) == 2


def test_split_casewhen_procedure(load_file):
    # see issue580
    stmts = sqlparse.split(load_file('casewhen_procedure.sql'))
    assert len(stmts) == 2


def test_split_cursor_declare():
    sql = ('DECLARE CURSOR "foo" AS SELECT 1;\n'
           'SELECT 2;')
    stmts = sqlparse.split(sql)
    assert len(stmts) == 2


def test_split_if_function():  # see issue 33
    # don't let IF as a function confuse the splitter
    sql = ('CREATE TEMPORARY TABLE tmp '
           'SELECT IF(a=1, a, b) AS o FROM one; '
           'SELECT t FROM two')
    stmts = sqlparse.split(sql)
    assert len(stmts) == 2


def test_split_stream():
    stream = StringIO("SELECT 1; SELECT 2;")
    stmts = sqlparse.parsestream(stream)
    assert isinstance(stmts, types.GeneratorType)
    assert len(list(stmts)) == 2


def test_split_encoding_parsestream():
    stream = StringIO("SELECT 1; SELECT 2;")
    stmts = list(sqlparse.parsestream(stream))
    assert isinstance(stmts[0].tokens[0].value, str)


def test_split_unicode_parsestream():
    stream = StringIO('SELECT ö')
    stmts = list(sqlparse.parsestream(stream))
    assert str(stmts[0]) == 'SELECT ö'


def test_split_simple():
    stmts = sqlparse.split('select * from foo; select * from bar;')
    assert len(stmts) == 2
    assert stmts[0] == 'select * from foo;'
    assert stmts[1] == 'select * from bar;'


def test_split_ignores_empty_newlines():
    stmts = sqlparse.split('select foo;\nselect bar;\n')
    assert len(stmts) == 2
    assert stmts[0] == 'select foo;'
    assert stmts[1] == 'select bar;'


def test_split_quotes_with_new_line():
    stmts = sqlparse.split('select "foo\nbar"')
    assert len(stmts) == 1
    assert stmts[0] == 'select "foo\nbar"'

    stmts = sqlparse.split("select 'foo\n\bar'")
    assert len(stmts) == 1
    assert stmts[0] == "select 'foo\n\bar'"


def test_split_mysql_handler_for(load_file):
    # see issue581
    stmts = sqlparse.split(load_file('mysql_handler.sql'))
    assert len(stmts) == 2


@pytest.mark.parametrize('sql, expected', [
    ('select * from foo;', ['select * from foo']),
    ('select * from foo', ['select * from foo']),
    ('select * from foo; select * from bar;', [
        'select * from foo',
        'select * from bar',
    ]),
    ('  select * from foo;\n\nselect * from bar;\n\n\n\n', [
        'select * from foo',
        'select * from bar',
    ]),
    ('select * from foo\n\n;  bar', ['select * from foo', 'bar']),
])
def test_split_strip_semicolon(sql, expected):
    stmts = sqlparse.split(sql, strip_semicolon=True)
    assert len(stmts) == len(expected)
    for idx, expectation in enumerate(expected):
        assert stmts[idx] == expectation


def test_split_strip_semicolon_procedure(load_file):
    stmts = sqlparse.split(load_file('mysql_handler.sql'),
                           strip_semicolon=True)
    assert len(stmts) == 2
    assert stmts[0].endswith('end')
    assert stmts[1].endswith('end')

@pytest.mark.parametrize('sql, num', [
    ('USE foo;\nGO\nSELECT 1;\nGO', 4),
    ('SELECT * FROM foo;\nGO', 2),
    ('USE foo;\nGO 2\nSELECT 1;', 3)
])
def test_split_go(sql, num):  # issue762
    stmts = sqlparse.split(sql)
    assert len(stmts) == num


def test_split_multiple_case_in_begin(load_file):  # issue784
    stmts = sqlparse.split(load_file('multiple_case_in_begin.sql'))
    assert len(stmts) == 1
