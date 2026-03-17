import re
import unittest

from rstr import Rstr


class TestXeger(unittest.TestCase):
    def setUp(self) -> None:
        self.rs = Rstr()

    def test_literals(self) -> None:
        pattern = r'foo'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_dot(self) -> None:
        '''
        Verify that the dot character doesn't produce newlines.
        See: https://bitbucket.org/leapfrogdevelopment/rstr/issue/1/
        '''
        pattern = r'.+'
        for _ in range(100):
            assert re.match(pattern, self.rs.xeger(pattern))

    def test_digit(self) -> None:
        pattern = r'\d'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_nondigits(self) -> None:
        pattern = r'\D'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_literal_with_repeat(self) -> None:
        pattern = r'A{3}'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_literal_with_range_repeat(self) -> None:
        pattern = r'A{2,5}'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_word(self) -> None:
        pattern = r'\w'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_nonword(self) -> None:
        pattern = r'\W'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_or(self) -> None:
        pattern = r'foo|bar'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_or_with_subpattern(self) -> None:
        pattern = r'(foo|bar)'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_range(self) -> None:
        pattern = r'[A-F]'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_character_group(self) -> None:
        pattern = r'[ABC]'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_carot(self) -> None:
        pattern = r'^foo'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_dollarsign(self) -> None:
        pattern = r'foo$'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_not_literal(self) -> None:
        pattern = r'[^a]'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_negation_group(self) -> None:
        pattern = r'[^AEIOU]'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_lookahead(self) -> None:
        pattern = r'foo(?=bar)'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_lookbehind(self) -> None:
        pattern = r'(?<=foo)bar'
        assert re.search(pattern, self.rs.xeger(pattern))

    def test_backreference(self) -> None:
        pattern = r'(foo|bar)baz\1'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_zero_or_more_greedy(self) -> None:
        pattern = r'a*'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_zero_or_more_non_greedy(self) -> None:
        pattern = r'a*?'
        assert re.match(pattern, self.rs.xeger(pattern))
