# -*- coding: utf-8 -*-
import io
import os
import sys
import unittest
from contextlib import contextmanager

from slugify import slugify
from slugify import smart_truncate
from slugify.__main__ import slugify_params, parse_args


class TestSlugification(unittest.TestCase):

    def test_extraneous_seperators(self):

        txt = "This is a test ---"
        r = slugify(txt)
        self.assertEqual(r, "this-is-a-test")

        txt = "___This is a test ---"
        r = slugify(txt)
        self.assertEqual(r, "this-is-a-test")

        txt = "___This is a test___"
        r = slugify(txt)
        self.assertEqual(r, "this-is-a-test")

    def test_non_word_characters(self):
        txt = "This -- is a ## test ---"
        r = slugify(txt)
        self.assertEqual(r, "this-is-a-test")

    def test_phonetic_conversion_of_eastern_scripts(self):
        txt = '影師嗎'
        r = slugify(txt)
        self.assertEqual(r, "ying-shi-ma")

    def test_accented_text(self):
        txt = 'C\'est déjà l\'été.'
        r = slugify(txt)
        self.assertEqual(r, "c-est-deja-l-ete")

        txt = 'Nín hǎo. Wǒ shì zhōng guó rén'
        r = slugify(txt)
        self.assertEqual(r, "nin-hao-wo-shi-zhong-guo-ren")

    def test_accented_text_with_non_word_characters(self):
        txt = 'jaja---lol-méméméoo--a'
        r = slugify(txt)
        self.assertEqual(r, "jaja-lol-mememeoo-a")

    def test_cyrillic_text(self):
        txt = 'Компьютер'
        r = slugify(txt)
        self.assertEqual(r, "kompiuter")

    def test_max_length(self):
        txt = 'jaja---lol-méméméoo--a'
        r = slugify(txt, max_length=9)
        self.assertEqual(r, "jaja-lol")

        txt = 'jaja---lol-méméméoo--a'
        r = slugify(txt, max_length=15)
        self.assertEqual(r, "jaja-lol-mememe")

    def test_max_length_cutoff_not_required(self):
        txt = 'jaja---lol-méméméoo--a'
        r = slugify(txt, max_length=50)
        self.assertEqual(r, "jaja-lol-mememeoo-a")

    def test_word_boundary(self):
        txt = 'jaja---lol-méméméoo--a'
        r = slugify(txt, max_length=15, word_boundary=True)
        self.assertEqual(r, "jaja-lol-a")

        txt = 'jaja---lol-méméméoo--a'
        r = slugify(txt, max_length=17, word_boundary=True)
        self.assertEqual(r, "jaja-lol-mememeoo")

        txt = 'jaja---lol-méméméoo--a'
        r = slugify(txt, max_length=18, word_boundary=True)
        self.assertEqual(r, "jaja-lol-mememeoo")

        txt = 'jaja---lol-méméméoo--a'
        r = slugify(txt, max_length=19, word_boundary=True)
        self.assertEqual(r, "jaja-lol-mememeoo-a")

    def test_custom_separator(self):
        txt = 'jaja---lol-méméméoo--a'
        r = slugify(txt, max_length=20, word_boundary=True, separator=".")
        self.assertEqual(r, "jaja.lol.mememeoo.a")

    def test_multi_character_separator(self):
        txt = 'jaja---lol-méméméoo--a'
        r = slugify(txt, max_length=20, word_boundary=True, separator="ZZZZZZ")
        self.assertEqual(r, "jajaZZZZZZlolZZZZZZmememeooZZZZZZa")

    def test_save_order(self):
        txt = 'one two three four five'
        r = slugify(txt, max_length=13, word_boundary=True, save_order=True)
        self.assertEqual(r, "one-two-three")

        txt = 'one two three four five'
        r = slugify(txt, max_length=13, word_boundary=True, save_order=False)
        self.assertEqual(r, "one-two-three")

        txt = 'one two three four five'
        r = slugify(txt, max_length=12, word_boundary=True, save_order=False)
        self.assertEqual(r, "one-two-four")

        txt = 'one two three four five'
        r = slugify(txt, max_length=12, word_boundary=True, save_order=True)
        self.assertEqual(r, "one-two")

    def test_stopword_removal(self):
        txt = 'this has a stopword'
        r = slugify(txt, stopwords=['stopword'])
        self.assertEqual(r, 'this-has-a')

    def test_stopword_removal_casesensitive(self):
        txt = 'thIs Has a stopword Stopword'
        r = slugify(txt, stopwords=['Stopword'], lowercase=False)
        self.assertEqual(r, 'thIs-Has-a-stopword')

    def test_multiple_stopword_occurances(self):
        txt = 'the quick brown fox jumps over the lazy dog'
        r = slugify(txt, stopwords=['the'])
        self.assertEqual(r, 'quick-brown-fox-jumps-over-lazy-dog')

    def test_differently_cased_stopword_match(self):
        txt = 'Foo A FOO B foo C'
        r = slugify(txt, stopwords=['foo'])
        self.assertEqual(r, 'a-b-c')

        txt = 'Foo A FOO B foo C'
        r = slugify(txt, stopwords=['FOO'])
        self.assertEqual(r, 'a-b-c')

    def test_multiple_stopwords(self):
        txt = 'the quick brown fox jumps over the lazy dog in a hurry'
        r = slugify(txt, stopwords=['the', 'in', 'a', 'hurry'])
        self.assertEqual(r, 'quick-brown-fox-jumps-over-lazy-dog')

    def test_stopwords_with_different_separator(self):
        txt = 'the quick brown fox jumps over the lazy dog'
        r = slugify(txt, stopwords=['the'], separator=' ')
        self.assertEqual(r, 'quick brown fox jumps over lazy dog')

    def test_html_entities_on(self):
        txt = 'foo &amp; bar'
        r = slugify(txt)
        self.assertEqual(r, 'foo-bar')

    def test_html_entities_off(self):
        txt = 'foo &amp; bar'
        r = slugify(txt, entities=False)
        self.assertEqual(r, 'foo-amp-bar')

    def test_html_decimal_on(self):
        txt = '&#381;'
        r = slugify(txt, decimal=True)
        self.assertEqual(r, 'z')

    def test_html_decimal_off(self):
        txt = '&#381;'
        r = slugify(txt, entities=False, decimal=False)
        self.assertEqual(r, '381')

    def test_html_hexadecimal_on(self):
        txt = '&#x17D;'
        r = slugify(txt, hexadecimal=True)
        self.assertEqual(r, 'z')

    def test_html_hexadecimal_off(self):
        txt = '&#x17D;'
        r = slugify(txt, hexadecimal=False)
        self.assertEqual(r, 'x17d')

    def test_starts_with_number(self):
        txt = '10 amazing secrets'
        r = slugify(txt)
        self.assertEqual(r, '10-amazing-secrets')

    def test_contains_numbers(self):
        txt = 'buildings with 1000 windows'
        r = slugify(txt)
        self.assertEqual(r, 'buildings-with-1000-windows')

    def test_ends_with_number(self):
        txt = 'recipe number 3'
        r = slugify(txt)
        self.assertEqual(r, 'recipe-number-3')

    def test_numbers_only(self):
        txt = '404'
        r = slugify(txt)
        self.assertEqual(r, '404')

    def test_numbers_and_symbols(self):
        txt = '1,000 reasons you are #1'
        r = slugify(txt)
        self.assertEqual(r, '1000-reasons-you-are-1')

    def test_regex_pattern_keep_underscore(self):
        txt = "___This is a test___"
        regex_pattern = r'[^-a-z0-9_]+'
        r = slugify(txt, regex_pattern=regex_pattern)
        self.assertEqual(r, "___this-is-a-test___")

    def test_regex_pattern_keep_underscore_with_underscore_as_separator(self):
        """
        The regex_pattern turns the power to the caller.
        Hence the caller must ensure that a custom separator doesn't clash
        with the regex_pattern.
        """
        txt = "___This is a test___"
        regex_pattern = r'[^-a-z0-9_]+'
        r = slugify(txt, separator='_', regex_pattern=regex_pattern)
        self.assertNotEqual(r, "_this_is_a_test_")

    def test_replacements(self):
        txt = '10 | 20 %'
        r = slugify(txt, replacements=[['|', 'or'], ['%', 'percent']])
        self.assertEqual(r, "10-or-20-percent")

        txt = 'I ♥ 🦄'
        r = slugify(txt, replacements=[['♥', 'amour'], ['🦄', 'licorne']])
        self.assertEqual(r, "i-amour-licorne")

    def test_replacements_german_umlaut_custom(self):
        txt = 'ÜBER Über German Umlaut'
        r = slugify(txt, replacements=[['Ü', 'UE'], ['ü', 'ue']])
        self.assertEqual(r, "ueber-ueber-german-umlaut")


class TestUtils(unittest.TestCase):

    def test_smart_truncate_no_max_length(self):
        txt = '1,000 reasons you are #1'
        r = smart_truncate(txt)
        self.assertEqual(r, txt)

    def test_smart_truncate_no_seperator(self):
        txt = '1,000 reasons you are #1'
        r = smart_truncate(txt, max_length=100, separator='_')
        self.assertEqual(r, txt)


PY3 = sys.version_info.major == 3


@contextmanager
def captured_stderr():
    backup = sys.stderr
    sys.stderr = io.StringIO() if PY3 else io.BytesIO()
    try:
        yield sys.stderr
    finally:
        sys.stderr = backup


@contextmanager
def loaded_stdin(contents):
    backup = sys.stdin
    sys.stdin = io.StringIO(contents) if PY3 else io.BytesIO(contents)
    try:
        yield sys.stdin
    finally:
        sys.stdin = backup


class TestCommandParams(unittest.TestCase):
    DEFAULTS = {
        'entities': True,
        'decimal': True,
        'hexadecimal': True,
        'max_length': 0,
        'word_boundary': False,
        'save_order': False,
        'separator': '-',
        'stopwords': None,
        'lowercase': True,
        'replacements': None
    }

    def get_params_from_cli(self, *argv):
        args = parse_args([None] + list(argv))
        return slugify_params(args)

    def make_params(self, **values):
        return dict(self.DEFAULTS, **values)

    def assertParamsMatch(self, expected, checked):
        reduced_checked = {}
        for key in expected.keys():
            reduced_checked[key] = checked[key]
        self.assertEqual(expected, reduced_checked)

    def test_defaults(self):
        params = self.get_params_from_cli()
        self.assertParamsMatch(self.DEFAULTS, params)

    def test_negative_flags(self):
        params = self.get_params_from_cli('--no-entities', '--no-decimal', '--no-hexadecimal', '--no-lowercase')
        expected = self.make_params(entities=False, decimal=False, hexadecimal=False, lowercase=False)
        self.assertFalse(expected['lowercase'])
        self.assertFalse(expected['word_boundary'])
        self.assertParamsMatch(expected, params)

    def test_affirmative_flags(self):
        params = self.get_params_from_cli('--word-boundary', '--save-order')
        expected = self.make_params(word_boundary=True, save_order=True)
        self.assertParamsMatch(expected, params)

    def test_valued_arguments(self):
        params = self.get_params_from_cli('--stopwords', 'abba', 'beatles', '--max-length', '98', '--separator', '+')
        expected = self.make_params(stopwords=['abba', 'beatles'], max_length=98, separator='+')
        self.assertParamsMatch(expected, params)

    def test_replacements_right(self):
        params = self.get_params_from_cli('--replacements', 'A->B', 'C->D')
        expected = self.make_params(replacements=[['A', 'B'], ['C', 'D']])
        self.assertParamsMatch(expected, params)

    def test_replacements_wrong(self):
        with self.assertRaises(SystemExit) as err, captured_stderr() as cse:
            self.get_params_from_cli('--replacements', 'A--B')
        self.assertEqual(err.exception.code, 2)
        self.assertIn("Replacements must be of the form: ORIGINAL->REPLACED", cse.getvalue())

    def test_text_in_cli(self):
        params = self.get_params_from_cli('Cool Text')
        expected = self.make_params(text='Cool Text')
        self.assertParamsMatch(expected, params)

    def test_text_in_cli_multi(self):
        params = self.get_params_from_cli('Cool', 'Text')
        expected = self.make_params(text='Cool Text')
        self.assertParamsMatch(expected, params)

    def test_text_in_stdin(self):
        with loaded_stdin("Cool Stdin"):
            params = self.get_params_from_cli('--stdin')
        expected = self.make_params(text='Cool Stdin')
        self.assertParamsMatch(expected, params)

    def test_two_text_sources_fails(self):
        with self.assertRaises(SystemExit) as err, captured_stderr() as cse:
            self.get_params_from_cli('--stdin', 'Text')
        self.assertEqual(err.exception.code, 2)
        self.assertIn("Input strings and --stdin cannot work together", cse.getvalue())

    def test_multivalued_options_with_text(self):
        text = "the quick brown fox jumps over the lazy dog in a hurry"
        cli_args = "--stopwords the in a hurry -- {}".format(text).split()
        params = self.get_params_from_cli(*cli_args)
        self.assertEqual(params['text'], text)
        self.assertEqual(params['stopwords'], ['the', 'in', 'a', 'hurry'])


if __name__ == '__main__':
    unittest.main()
