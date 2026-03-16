import unittest
from braceexpand import braceexpand, UnbalancedBracesError

class BraceExpand(unittest.TestCase):
    tests = [
            ('{1,2}', ['1', '2']),
            ('{1}', ['{1}']),
            ('{1,2{}}', ['1', '2{}']),
            ('}{', ['}{']),
            ('a{b,c}d{e,f}', ['abde', 'abdf', 'acde', 'acdf']),
            ('a{b,c{d,e,}}', ['ab', 'acd', 'ace', 'ac']),
            ('a{b,{c,{d,e}}}', ['ab', 'ac', 'ad', 'ae']),
            ('{{a,b},{c,d}}', ['a', 'b', 'c', 'd']),
            ('{7..10}', ['7', '8', '9', '10']),
            ('{10..7}', ['10', '9', '8', '7']),
            ('{1..5..2}', ['1', '3', '5']),
            ('{5..1..2}', ['5', '3', '1']),
            ('{1..3..0}', ['1', '2', '3']),
            ('{1..3..-0}', ['1', '2', '3']),
            ('{a..b..0}', ['a', 'b']),
            ('{a..b..-0}', ['a', 'b']),
            ('{07..10}', ['07', '08', '09', '10']),
            ('{7..010}', ['007', '008', '009', '010']),
            ('{1..-2}', ['1', '0', '-1', '-2']),
            ('{01..-2}', ['01', '00', '-1', '-2']),
            ('{1..-02}', ['001', '000', '-01', '-02']),
            ('{-01..3..2}', ['-01', '001', '003']),
            ('{a..e}', ['a', 'b', 'c', 'd', 'e']),
            ('{a..e..2}', ['a', 'c', 'e']),
            ('{e..a}', ['e', 'd', 'c', 'b', 'a']),
            ('{e..a..2}', ['e', 'c', 'a']),
            ('{1..a}', ['{1..a}']),
            ('{a..1}', ['{a..1}']),
            ('{1..1}', ['1']),
            ('{a..a}', ['a']),
            ('{,}', ['', '']),
            ('{Z..a}', ['Z', 'a']),
            ('{a..Z}', ['a', 'Z']),
            ('{A..z}', list('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz')),
            ('{z..A}', list('zyxwvutsrqponmlkjihgfedcbaZYXWVUTSRQPONMLKJIHGFEDCBA')),
            ('{a.{b,c}}', ['{a.b}', '{a.c}']),
            ('{a.{1..2}}', ['{a.1}', '{a.2}']),
            ('{{{,}}}', ['{{}}', '{{}}']),
    ]

    unbalanced_tests = [
            # Unbalanced braces
            '{{1,2}',  # Bash: {1 {2
            '{1,2}}',  # Bash: 1} 2}
            '{1},2}',  # Bash: 1} 2
            '{1,{2}',  # Bash: {1,{2}
            '{}1,2}',  # Bash: }1 2
            '{1,2{}',  # Bash: {1,2{}
            '}{1,2}',  # Bash: }1 }2
            '{1,2}{',  # Bash: 1{ 2{
    ]

    escape_tests = [
            ('\\{1,2\\}', ['{1,2}']),
            ('{1\\,2}',   ['{1,2}']),

            ('\\}{1,2}', ['}1', '}2']),
            ('\\{{1,2}', ['{1', '{2']),
            ('{1,2}\\}', ['1}', '2}']),
            ('{1,2}\\{', ['1{', '2{']),

            ('{\\,1,2}', [',1', '2']),
            ('{1\\,,2}', ['1,', '2']),
            ('{1,\\,2}', ['1', ',2']),
            ('{1,2\\,}', ['1', '2,']),

            ('\\\\{1,2}', ['\\1', '\\2']),

            ('\\{1..2\\}', ['{1..2}']),
    ]

    no_escape_tests = [
            ('\\{1,2}', ['\\1', '\\2']),
            ('{1,2\\}', ['1', '2\\']),
            ('{1\\,2}', ['1\\', '2']),

            ('{\\,1,2}', ['\\', '1', '2']),
            ('{1\\,,2}', ['1\\', '', '2']),
            ('{1,\\,2}', ['1', '\\', '2']),
            ('{1,2\\,}', ['1', '2\\', '']),

            ('\\{1..2\\}', ['\\{1..2\\}']),
    ]

    def test_braceexpand(self):
        for pattern, expected in self.tests:
            result = list(braceexpand(pattern))
            self.assertEqual(expected, result)

    def test_braceexpand_unbalanced(self):
        for pattern in self.unbalanced_tests:
            self.assertRaises(UnbalancedBracesError, braceexpand, pattern)

    def test_braceexpand_escape(self):
        for pattern, expected in self.escape_tests:
            result = list(braceexpand(pattern, escape=True))
            self.assertEqual(expected, result)

    def test_braceexpand_no_escape(self):
        for pattern, expected in self.no_escape_tests:
            result = list(braceexpand(pattern, escape=False))
            self.assertEqual(expected, result)

    def test_zero_padding(self):
        result = list(braceexpand('{00..10}'))
        self.assertEqual(result[:2], ['00', '01'])

        result = list(braceexpand('{10..00}'))
        self.assertEqual(result[-2:], ['01', '00'])

        result = list(braceexpand('{0..010}'))
        self.assertEqual(result[:2], ['000', '001'])

        result = braceexpand('{01..1000}')
        self.assertEqual(next(result), '0001')

    def test_zero_single_digit(self):
        result = list(braceexpand('{0..10}'))
        self.assertEqual(result[:2], ['0', '1'])

        result = list(braceexpand('{10..0}'))
        self.assertEqual(result[-2:], ['1', '0'])

    def test_negative_zero(self):
        result = list(braceexpand('{-0..1}'))
        self.assertEqual(result, ['0', '1'])

        result = list(braceexpand('{1..-0}'))
        self.assertEqual(result, ['1', '0'])

        result = list(braceexpand('{0..-0}'))
        self.assertEqual(result, ['0'])

if __name__ == '__main__':
    unittest.main()
