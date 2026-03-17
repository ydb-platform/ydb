import re
import unittest

from rstr.rstr_base import Rstr


class TestXeger(unittest.TestCase):
    def setUp(self):
        self.rs = Rstr()

    def test_literals(self):
        pattern = r'foo'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_dot(self):
        """
        Verify that the dot character doesn't produce newlines.
        See: https://bitbucket.org/leapfrogdevelopment/rstr/issue/1/
        """
        pattern = r'.+'
        for i in range(100):
            assert re.match(pattern, self.rs.xeger(pattern))

    def test_digit(self):
        pattern = r'\d'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_nondigits(self):
        pattern = r'\D'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_literal_with_repeat(self):
        pattern = r'A{3}'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_literal_with_range_repeat(self):
        pattern = r'A{2, 5}'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_word(self):
        pattern = r'\w'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_nonword(self):
        pattern = r'\W'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_or(self):
        pattern = r'foo|bar'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_or_with_subpattern(self):
        pattern = r'(foo|bar)'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_range(self):
        pattern = r'[A-F]'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_character_group(self):
        pattern = r'[ABC]'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_carot(self):
        pattern = r'^foo'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_dollarsign(self):
        pattern = r'foo$'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_not_literal(self):
        pattern = r'[^a]'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_negation_group(self):
        pattern = r'[^AEIOU]'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_lookahead(self):
        pattern = r'foo(?=bar)'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_lookbehind(self):
        pattern = r'(?<=foo)bar'
        assert re.search(pattern, self.rs.xeger(pattern))

    def test_backreference(self):
        pattern = r'(foo|bar)baz\1'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_zero_or_more_greedy(self):
        pattern = r'a*'
        assert re.match(pattern, self.rs.xeger(pattern))

    def test_zero_or_more_non_greedy(self):
        pattern = r'a*?'
        assert re.match(pattern, self.rs.xeger(pattern))
