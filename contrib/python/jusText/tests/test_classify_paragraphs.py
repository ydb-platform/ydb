# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division, print_function, unicode_literals

from justext.core import PathInfo, classify_paragraphs
from justext.paragraph import Paragraph


class TestClassifyParagraphs():

    def _paragraph(self, **kwargs):
        path = PathInfo().append("body").append("p")
        paragraph = Paragraph(path)

        for n, v in kwargs.items():
            if n == "text":
                paragraph.append_text(v)
            else:
                setattr(paragraph, n, v)

        return paragraph

    def test_max_link_density(self):
        paragraphs = [
            self._paragraph(text="0123456789"*2, chars_count_in_links=0),
            self._paragraph(text="0123456789"*2, chars_count_in_links=20),
            self._paragraph(text="0123456789"*8, chars_count_in_links=40),
            self._paragraph(text="0123456789"*8, chars_count_in_links=39),
            self._paragraph(text="0123456789"*8, chars_count_in_links=41),
        ]

        classify_paragraphs(paragraphs, (), max_link_density=0.5)

        assert paragraphs[0].cf_class == "short"
        assert paragraphs[1].cf_class == "bad"
        assert paragraphs[2].cf_class == "bad"
        assert paragraphs[3].cf_class == "bad"
        assert paragraphs[4].cf_class == "bad"

    def test_length_low(self):
        paragraphs = [
            self._paragraph(text="0 1 2 3 4 5 6 7 8 9"*2, chars_count_in_links=0),
            self._paragraph(text="0 1 2 3 4 5 6 7 8 9"*2, chars_count_in_links=20),
        ]

        classify_paragraphs(paragraphs, (), max_link_density=1, length_low=1000)

        assert paragraphs[0].cf_class == "short"
        assert paragraphs[1].cf_class == "bad"

    def test_stopwords_high(self):
        paragraphs = [
            self._paragraph(text="0 1 2 3 4 5 6 7 8 9"),
            self._paragraph(text="0 1 2 3 4 5 6 7 8 9"*2),
        ]

        classify_paragraphs(
            paragraphs,
            ("0",),
            max_link_density=1,
            length_low=0,
            stopwords_high=0,
            length_high=20
        )

        assert paragraphs[0].cf_class == "neargood"
        assert paragraphs[1].cf_class == "good"

    def test_stopwords_low(self):
        paragraphs = [
            self._paragraph(text="0 0 0 0 1 2 3 4 5 6 7 8 9"),
            self._paragraph(text="0 1 2 3 4 5 6 7 8 9"),
            self._paragraph(text="1 2 3 4 5 6 7 8 9"),
        ]

        classify_paragraphs(
            paragraphs,
            ("0", "1",),
            max_link_density=1,
            length_low=0,
            stopwords_high=1000,
            stopwords_low=0.2
        )

        assert paragraphs[0].cf_class == "neargood"
        assert paragraphs[1].cf_class == "neargood"
        assert paragraphs[2].cf_class == "bad"
