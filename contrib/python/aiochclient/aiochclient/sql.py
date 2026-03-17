import re

import sqlparse.keywords
from sqlparse import tokens
from sqlparse.keywords import KEYWORDS, KEYWORDS_COMMON
from sqlparse.lexer import Lexer


class Where(sqlparse.sql.TokenList):
    M_OPEN = sqlparse.tokens.Keyword, 'WHERE'
    M_CLOSE = (
        sqlparse.tokens.Keyword,
        (
            'ORDER BY',
            'GROUP BY',
            'LIMIT',
            'UNION',
            'UNION ALL',
            'EXCEPT',
            'HAVING',
            'RETURNING',
            'INTO',
            'FORMAT',
        ),
    )


sqlparse.sql.Where = Where

SQLPARSE_LEXER = Lexer.get_default_instance()
SQLPARSE_LEXER.clear()

# Regex
SQLPARSE_LEXER.set_SQL_REGEX(
    [
        (r'(FORMAT)\b', sqlparse.tokens.Keyword),
        (r'(DESCRIBE|SHOW|EXISTS)\b', sqlparse.tokens.Keyword.DML),
        *sqlparse.keywords.SQL_REGEX,
    ]
)

# Keywords
SQLPARSE_LEXER.add_keywords(KEYWORDS_COMMON)
SQLPARSE_LEXER.add_keywords(KEYWORDS)
SQLPARSE_LEXER.add_keywords(
    {
        'FORMAT': tokens.Keyword,
        'EXISTS': tokens.Keyword.DML,
        'DESCRIBE': tokens.Keyword.DML,
        'SHOW': tokens.Keyword.DML,
    }
)
