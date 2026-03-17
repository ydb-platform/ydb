import pytest

from sqlparse import tokens
from sqlparse.lexer import Lexer


class TestSQLREGEX:
    @pytest.mark.parametrize('number', ['1.0', '-1.0',
                                        '1.', '-1.',
                                        '.1', '-.1'])
    def test_float_numbers(self, number):
        ttype = next(tt for action, tt in Lexer.get_default_instance()._SQL_REGEX if action(number))
        assert tokens.Number.Float == ttype
