import unittest

from ydb.library.benchmarks.template import Builder, ResultFormatter


class Test(unittest.TestCase):
    def test_create(self):
        Builder()

    def test_add(self):
        b = Builder()
        b.add("name", "text")
        text = b.build("name")
        self.assertEqual("text", text)

    def test_include_from_resource(self):
        b = Builder()
        b.add("name", """
{% include 'test.txt' %}
Content

""")
        text = b.build("name")
        expected = """
Text
Content
"""
        self.assertEqual(expected, text)

    def test_add_vars(self):
        b = Builder()
        b.add("name", "{{var}}")
        b.add_vars({"var": "text"})
        text = b.build("name")
        self.assertEqual("text", text)

    def test_include(self):
        b = Builder()
        b.add("include_name", "IncludeText")
        b.add("name", """
{% include 'include_name' %}
Content

""")
        text = b.build("name")
        expected = """
IncludeText
Content
"""
        self.assertEqual(expected, text)

    def test_linked_include(self):
        b = Builder()
        b.add("include_name", "IncludeText")
        b.add_link("include_name1", "include_name")
        b.add("name", """
{% include 'include_name1' %}
Content

""")
        text = b.build("name")
        expected = """
IncludeText
Content
"""
        self.assertEqual(expected, text)

    def test_expose_var_from_include(self):
        b = Builder()
        b.add("include_name", '{% set var = "VAR" %}')
        b.add("name", "{% include 'include_name' %}{{var}}")
        text = b.build("name", True)
        expected = "VAR"
        self.assertEqual(expected, text)

    def test_expose_var_from_var(self):
        b = Builder()
        b.add_vars({"var": "VAR"})
        b.add("name", '{% set abc = var + "ABC" %}{{abc}}')
        text = b.build("name", True)
        expected = "VARABC"
        self.assertEqual(expected, text)

    def test_result_formatter(self):
        d = {
            'data': [{
                'Write': [{
                    'Type': [
                        'ListType', [
                            'StructType', [
                                ['cntrycode', ['DataType', 'String']],
                                ['numcust', ['DataType', 'Uint64']],
                                ['totacctbal', ['DataType', 'Double']]]]],
                    'Data': [
                        ['15', '893', '6702431.719999995'],
                        ['25', '877', '6511759.129999992'],
                        ['26', '859', '6394689.130000009'],
                        ['28', '909', '6710689.259999996'],
                        ['29', '948', '7158866.629999996'],
                        ['30', '909', '6808436.1299999915'],
                        ['31', '922', '6806670.179999996'],
                        ['0', '0', None]]}],
                'Position': {'Column': '1', 'Row': '55', 'File': '<main>'}}],
            'errors': [],
            'id': '645e199819d7146f01c11bac',
            'issues': [],
            'status': 'COMPLETED',
            'updatedAt': '2023-05-12T10:49:07.967Z',
            'version': 1000000}

        f = ResultFormatter("%.2f")
        f.format(d['data'])

        expected = {
            'data': [{
                'Write': [{
                    'Type': [
                        'ListType', [
                            'StructType', [
                                ['cntrycode', ['DataType', 'String']],
                                ['numcust', ['DataType', 'Uint64']],
                                ['totacctbal', ['DataType', 'Double']]]]],
                    'Data': [
                        ['15', '893', '6702431.72'],
                        ['25', '877', '6511759.13'],
                        ['26', '859', '6394689.13'],
                        ['28', '909', '6710689.26'],
                        ['29', '948', '7158866.63'],
                        ['30', '909', '6808436.13'],
                        ['31', '922', '6806670.18'],
                        ['0', '0', None]]}],
                'Position': {'Column': '1', 'Row': '55', 'File': '<main>'}}],
            'errors': [],
            'id': '645e199819d7146f01c11bac',
            'issues': [],
            'status': 'COMPLETED',
            'updatedAt': '2023-05-12T10:49:07.967Z',
            'version': 1000000}

        self.assertEqual(expected, d)

    def test_result_formatter_optional(self):
        d = {
            'data': [{
                'Write': [{
                    'Type': [
                        'ListType', [
                            'StructType', [
                                ['cntrycode', ['DataType', 'String']],
                                ['numcust', ['OptionalType', ['DataType', 'Uint64']]],
                                ['totacctbal', ['OptionalType', ['DataType', 'Double']]]]]],
                    'Data': [
                        ['15', ['893'], ['6702431.719999995']],
                        ['16', [], []],
                    ]}]}]}

        expected = {
            'data': [{
                'Write': [{
                    'Type': [
                        'ListType', [
                            'StructType', [
                                ['cntrycode', ['DataType', 'String']],
                                ['numcust', ['OptionalType', ['DataType', 'Uint64']]],
                                ['totacctbal', ['OptionalType', ['DataType', 'Double']]]]]],
                    'Data': [
                        ['15', '893', '6702431.72'],
                        ['16', 'NULL', 'NULL'],
                    ]}]}]}

        f = ResultFormatter("%.2f")
        f.format(d['data'])
        self.assertEqual(expected, d)

    def test_result_formatter_zeros(self):
        d = {
            'data': [{
                'Write': [{
                    'Type': [
                        'ListType', [
                            'StructType', [
                                ['totacctbal', ['DataType', 'Double']]]]],
                    'Data': [
                        ['6702431.719999995'],
                        ['123.101'],
                        ['123.001'],
                        ['-0.001']
                    ]}]}]}

        expected = {
            'data': [{
                'Write': [{
                    'Type': [
                        'ListType', [
                            'StructType', [
                                ['totacctbal', ['DataType', 'Double']]]]],
                    'Data': [
                        ['6702431.72'],
                        ['123.1'],
                        ['123'],
                        ['0']
                    ]}]}]}

        f = ResultFormatter("%.2f")
        f.format(d['data'])
        self.assertEqual(expected, d)

    def test_result_formatter_dates(self):
        d = {
            'data': [{
                'Write': [{
                    'Type': [
                        'ListType', [
                            'StructType', [
                                ['totacctbal', ['DataType', 'Date']]]]],
                    'Data': [
                        ['9076'],
                        ['9667'],
                        [None]
                    ]}]}]}

        expected = {
            'data': [{
                'Write': [{
                    'Type': [
                        'ListType', [
                            'StructType', [
                                ['totacctbal', ['DataType', 'Date']]]]],
                    'Data': [
                        ['1994-11-07'],
                        ['1996-06-20'],
                        [None]
                    ]}]}]}

        f = ResultFormatter("%.2f")
        f.format(d['data'])
        self.assertEqual(expected, d)
