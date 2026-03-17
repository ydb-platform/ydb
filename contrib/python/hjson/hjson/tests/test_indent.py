from unittest import TestCase
import textwrap

import hjson as json
from hjson.compat import StringIO

class TestIndent(TestCase):
    def test_indent(self):
        h = [['blorpie'], ['whoops'], [], 'd-shtaeou', 'd-nthiouh',
             'i-vhbjkhnth',
             {'nifty': 87}, {'field': 'yes', 'morefield': False} ]

        expect = textwrap.dedent("""\
        [
        \t[
        \t\t"blorpie"
        \t],
        \t[
        \t\t"whoops"
        \t],
        \t[],
        \t"d-shtaeou",
        \t"d-nthiouh",
        \t"i-vhbjkhnth",
        \t{
        \t\t"nifty": 87
        \t},
        \t{
        \t\t"field": "yes",
        \t\t"morefield": false
        \t}
        ]""")


        d1 = json.dumpsJSON(h)
        d2 = json.dumpsJSON(h, indent='\t', sort_keys=True, separators=(',', ': '))
        d3 = json.dumpsJSON(h, indent='  ', sort_keys=True, separators=(',', ': '))
        d4 = json.dumpsJSON(h, indent=2, sort_keys=True, separators=(',', ': '))

        h1 = json.loads(d1)
        h2 = json.loads(d2)
        h3 = json.loads(d3)
        h4 = json.loads(d4)

        self.assertEqual(h1, h)
        self.assertEqual(h2, h)
        self.assertEqual(h3, h)
        self.assertEqual(h4, h)
        self.assertEqual(d3, expect.replace('\t', '  '))
        self.assertEqual(d4, expect.replace('\t', '  '))
        # NOTE: Python 2.4 textwrap.dedent converts tabs to spaces,
        #       so the following is expected to fail. Python 2.4 is not a
        #       supported platform in hjson 2.1.0+.
        self.assertEqual(d2, expect)

    def test_indent0(self):
        h = {3: 1}
        def check(indent, expected):
            d1 = json.dumpsJSON(h, indent=indent)
            self.assertEqual(d1, expected)

            sio = StringIO()
            json.dumpJSON(h, sio, indent=indent)
            self.assertEqual(sio.getvalue(), expected)

        # indent=0 should emit newlines
        check(0, '{\n"3": 1\n}')
        # indent=None is more compact
        check(None, '{"3": 1}')

    def test_separators(self):
        lst = [1,2,3,4]
        expect = '[\n1,\n2,\n3,\n4\n]'
        expect_spaces = '[\n1, \n2, \n3, \n4\n]'
        # Ensure that separators still works
        self.assertEqual(
            expect_spaces,
            json.dumpsJSON(lst, indent=0, separators=(', ', ': ')))
        # Force the new defaults
        self.assertEqual(
            expect,
            json.dumpsJSON(lst, indent=0, separators=(',', ': ')))
        # Added in 2.1.4
        self.assertEqual(
            expect,
            json.dumpsJSON(lst, indent=0))
