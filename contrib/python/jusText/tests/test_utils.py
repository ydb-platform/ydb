# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division, print_function, unicode_literals

import pytest

from justext.utils import is_blank, normalize_whitespace, get_stoplists, get_stoplist


class TestStringUtils():

    def test_empty_string_is_blank(self):
        assert is_blank("")

    def test_string_with_space_is_blank(self):
        assert is_blank(" ")

    def test_string_with_nobreak_space_is_blank(self):
        assert is_blank("\u00A0\t ")

    def test_string_with_narrow_nobreak_space_is_blank(self):
        assert is_blank("\u202F \t")

    def test_string_with_spaces_is_blank(self):
        assert is_blank("    ")

    def test_string_with_newline_is_blank(self):
        assert is_blank("\n")

    def test_string_with_tab_is_blank(self):
        assert is_blank("\t")

    def test_string_with_whitespace_is_blank(self):
        assert is_blank("\t\n ")

    def test_string_with_chars_is_not_blank(self):
        assert not is_blank("  #  ")

    def test_normalize_no_change(self):
        string = "a b c d e f g h i j k l m n o p q r s ..."
        assert string == normalize_whitespace(string)

    def test_normalize_dont_trim(self):
        string = "  a b c d e f g h i j k l m n o p q r s ...  "
        expected = " a b c d e f g h i j k l m n o p q r s ... "
        assert expected == normalize_whitespace(string)

    def test_normalize_newline_and_tab(self):
        string = "123 \n456\t\n"
        expected = "123\n456\n"
        assert expected == normalize_whitespace(string)

    def test_normalize_non_break_spaces(self):
        string = "\u00A0\t €\u202F \t"
        expected = " € "
        assert expected == normalize_whitespace(string)


class TestStoplistsUtils():

    def test_get_stopwords_list(self):
        stopwords = get_stoplists()

        assert stopwords == frozenset((
            "Afrikaans", "Albanian", "Arabic", "Aragonese", "Armenian",
            "Aromanian", "Asturian", "Azerbaijani", "Basque", "Belarusian",
            "Belarusian_Taraskievica", "Bengali", "Bishnupriya_Manipuri",
            "Bosnian", "Breton", "Bulgarian", "Catalan", "Cebuano", "Croatian",
            "Czech", "Danish", "Dutch", "English", "Esperanto", "Estonian",
            "Finnish", "French", "Galician", "Georgian", "German", "Greek",
            "Gujarati", "Haitian", "Hebrew", "Hindi", "Hungarian", "Chuvash",
            "Icelandic", "Ido", "Igbo", "Indonesian", "Irish", "Italian",
            "Javanese", "Kannada", "Kazakh", "Korean", "Kurdish", "Kyrgyz",
            "Latin", "Latvian", "Lithuanian", "Lombard", "Low_Saxon",
            "Luxembourgish", "Macedonian", "Malay", "Malayalam", "Maltese",
            "Marathi", "Neapolitan", "Nepali", "Newar", "Norwegian_Bokmal",
            "Norwegian_Nynorsk", "Occitan", "Persian", "Piedmontese", "Polish",
            "Portuguese", "Quechua", "Romanian", "Russian", "Samogitian",
            "Serbian", "Serbo_Croatian", "Sicilian", "Simple_English", "Slovak",
            "Slovenian", "Spanish", "Sundanese", "Swahili", "Swedish",
            "Tagalog", "Tamil", "Telugu", "Turkish", "Turkmen", "Ukrainian",
            "Urdu", "Uzbek", "Vietnamese", "Volapuk", "Walloon", "Waray_Waray",
            "Welsh", "West_Frisian", "Western_Panjabi", "Yoruba",
        ))

    def test_get_real_stoplist(self):
        stopwords = get_stoplist("Slovak")

        assert len(stopwords) > 0

    def test_get_missing_stoplist(self):
        with pytest.raises(ValueError):
            get_stoplist("Klingon")
