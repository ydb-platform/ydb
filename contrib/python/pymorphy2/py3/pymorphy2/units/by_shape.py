# -*- coding: utf-8 -*-
"""
Analyzer units that analyzes non-word tokes
-------------------------------------------
"""

from __future__ import absolute_import, unicode_literals, division

from pymorphy2.units.base import BaseAnalyzerUnit
from pymorphy2.shapes import is_latin, is_punctuation, is_roman_number


class _ShapeAnalyzer(BaseAnalyzerUnit):
    EXTRA_GRAMMEMES = []
    EXTRA_GRAMMEMES_CYR = []

    def __init__(self, score=0.9):
        self.score = score

    def init(self, morph):
        super(_ShapeAnalyzer, self).init(morph)

        for lat, cyr in zip(self.EXTRA_GRAMMEMES, self.EXTRA_GRAMMEMES_CYR):
            self.morph.TagClass.add_grammemes_to_known(lat, cyr)

    def parse(self, word, word_lower, seen_parses):
        shape = self.check_shape(word, word_lower)
        if not shape:
            return []

        methods = ((self, word),)
        return [(word_lower, self.get_tag(word, shape), word_lower, self.score, methods)]

    def tag(self, word, word_lower, seen_tags):
        shape = self.check_shape(word, word_lower)
        if not shape:
            return []
        return [self.get_tag(word, shape)]

    def get_lexeme(self, form):
        return [form]

    def normalized(self, form):
        return form

    # implement these 2 methods in a subclass:
    def check_shape(self, word, word_lower):
        raise NotImplementedError()

    def get_tag(self, word, shape):
        raise NotImplementedError()


class _SingleShapeAnalyzer(_ShapeAnalyzer):
    TAG_STR = None
    TAG_STR_CYR = None

    def init(self, morph):
        assert self.TAG_STR is not None
        assert self.TAG_STR_CYR is not None
        self.EXTRA_GRAMMEMES = self.TAG_STR.split(',')
        self.EXTRA_GRAMMEMES_CYR = self.TAG_STR_CYR.split(',')
        super(_SingleShapeAnalyzer, self).init(morph)
        self._tag = self.morph.TagClass(self.TAG_STR)

    def get_tag(self, word, shape):
        return self._tag


class PunctuationAnalyzer(_SingleShapeAnalyzer):
    """
    This analyzer tags punctuation marks as "PNCT".
    Example: "," -> PNCT
    """
    TAG_STR = 'PNCT'
    TAG_STR_CYR = 'ЗПР'  # aot.ru uses this name

    def check_shape(self, word, word_lower):
        return is_punctuation(word)


class LatinAnalyzer(_SingleShapeAnalyzer):
    """
    This analyzer marks latin words with "LATN" tag.
    Example: "pdf" -> LATN
    """
    TAG_STR = 'LATN'
    TAG_STR_CYR = 'ЛАТ'

    def check_shape(self, word, word_lower):
        return is_latin(word)


class NumberAnalyzer(_ShapeAnalyzer):
    """
    This analyzer marks integer numbers with "NUMB,int" or "NUMB,real" tags.
    Example: "12" -> NUMB,int; "12.4" -> NUMB,real

    .. note::

        Don't confuse it with "NUMR": "тридцать" -> NUMR

    """
    EXTRA_GRAMMEMES = ['NUMB', 'intg', 'real']
    EXTRA_GRAMMEMES_CYR = ['ЧИСЛО', 'цел', 'вещ']

    def init(self, morph):
        super(NumberAnalyzer, self).init(morph)
        self._tags = {
            'intg': morph.TagClass('NUMB,intg'),
            'real': morph.TagClass('NUMB,real'),
        }

    def check_shape(self, word, word_lower):
        try:
            int(word)
            return 'intg'
        except ValueError:
            try:
                float(word.replace(',', '.'))
                return 'real'
            except ValueError:
                pass
        return False

    def get_tag(self, word, shape):
        return self._tags[shape]


class RomanNumberAnalyzer(_SingleShapeAnalyzer):
    TAG_STR = 'ROMN'
    TAG_STR_CYR = 'РИМ'

    def check_shape(self, word, word_lower):
        return is_roman_number(word)
