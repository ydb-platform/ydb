import re
import unittest
import sys
import random

from rstr.rstr_base import Rstr

if sys.version_info[0] >= 3:
    unichr = chr
    xrange = range


class TestRstr(unittest.TestCase):
    def setUp(self):
        self.rs = Rstr()

    def test_specific_length(self):
        assert re.match('^A{5}$', self.rs.rstr('A', 5))

    def test_length_range(self):
        assert re.match('^A{11,20}$', self.rs.rstr('A', 11, 20))

    def test_custom_alphabet(self):
        assert re.match('^A{1,10}$', self.rs.rstr('AA'))

    def test_alphabet_as_list(self):
        assert re.match('^A{1,10}$', self.rs.rstr(['A', 'A']))

    def test_include(self):
        assert re.match('^[ABC]*@[ABC]*$', self.rs.rstr('ABC', include='@'))

    def test_exclude(self):
        for _ in xrange(0, 100):
            assert 'C' not in self.rs.rstr('ABC', exclude='C')

    def test_include_as_list(self):
        assert re.match('^[ABC]*@[ABC]*$', self.rs.rstr('ABC', include=["@"]))

    def test_exclude_as_list(self):
        for _ in xrange(0, 100):
            assert 'C' not in self.rs.rstr('ABC', exclude=['C'])


class TestSystemRandom(TestRstr):
    def setUp(self):
        self.rs = Rstr(random.SystemRandom())


class TestDigits(unittest.TestCase):
    def setUp(self):
        self.rs = Rstr()

    def test_all_digits(self):
        assert re.match('^\d{1,10}$', self.rs.digits())

    def test_digits_include(self):
        assert re.match('^\d*@\d*$', self.rs.digits(include='@'))

    def test_digits_exclude(self):
        for _ in xrange(0, 100):
            assert '5' not in self.rs.digits(exclude='5')


class TestNondigits(unittest.TestCase):
    def setUp(self):
        self.rs = Rstr()

    def test_nondigits(self):
        assert re.match('^\D{1,10}$', self.rs.nondigits())

    def test_nondigits_include(self):
        assert re.match('^\D*@\D*$', self.rs.nondigits(include='@'))

    def test_nondigits_exclude(self):
        for _ in xrange(0, 100):
            assert 'A' not in self.rs.nondigits(exclude='A')


class TestLetters(unittest.TestCase):
    def setUp(self):
        self.rs = Rstr()

    def test_letters(self):
        assert re.match('^[a-zA-Z]{1,10}$', self.rs.letters())

    def test_letters_include(self):
        assert re.match('^[a-zA-Z]*@[a-zA-Z]*$', self.rs.letters(include='@'))

    def test_letters_exclude(self):
        for _ in xrange(0, 100):
            assert 'A' not in self.rs.letters(exclude='A')


class TestCustomAlphabets(unittest.TestCase):
    def test_alphabet_at_instantiation(self):
        rs = Rstr(vowels='AEIOU')
        assert re.match('^[AEIOU]{1,10}$', rs.vowels())

    def test_add_alphabet(self):
        rs = Rstr()
        rs.add_alphabet('evens', '02468')
        assert re.match('^[02468]{1,10}$', rs.evens())


def main():
    unittest.main()


if __name__ == '__main__':
    main()
