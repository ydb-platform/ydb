# -*- coding: utf-8 -*-
"""
Dictionary analyzer unit
------------------------
"""
from __future__ import absolute_import, division, unicode_literals
import logging
from pymorphy2.units.base import BaseAnalyzerUnit


logger = logging.getLogger(__name__)


class DictionaryAnalyzer(BaseAnalyzerUnit):
    """
    Analyzer unit that analyzes word using dictionary.
    """

    def parse(self, word, word_lower, seen_parses):
        """
        Parse a word using this dictionary.
        """
        res = []
        para_data = self.dict.words.similar_items(word_lower, self.morph.char_substitutes)

        for fixed_word, parses in para_data:
            # `fixed_word` is a word with proper substitute (e.g. ё) letters

            for para_id, idx in parses:
                normal_form = self.dict.build_normal_form(para_id, idx, fixed_word)
                tag = self.dict.build_tag_info(para_id, idx)
                method = ((self, fixed_word, para_id, idx),)
                res.append((fixed_word, tag, normal_form, 1.0, method))

        # res.sort(key=lambda p: len(p[1]))  #  prefer simple parses
        return res

    def tag(self, word, word_lower, seen_tags):
        """
        Tag a word using this dictionary.
        """
        para_data = self.dict.words.similar_item_values(word_lower, self.morph.char_substitutes)

        # avoid extra attribute lookups
        paradigms = self.dict.paradigms
        gramtab = self.dict.gramtab

        # tag known word
        result = []
        for parse in para_data:
            for para_id, idx in parse:
                # result.append(self.build_tag_info(para_id, idx))
                # .build_tag_info is unrolled for speed
                paradigm = paradigms[para_id]
                paradigm_len = len(paradigm) // 3
                tag_id = paradigm[paradigm_len + idx]
                result.append(gramtab[tag_id])

        return result

    def get_lexeme(self, form):
        """
        Return a lexeme (given a parsed word).
        """
        fixed_word, tag, normal_form, score, methods_stack = form
        _, para_id, idx = self._extract_para_info(methods_stack)

        _para = self.dict.paradigms[para_id]
        stem = self.dict.build_stem(_para, idx, fixed_word)

        result = []
        paradigm = self.dict.build_paradigm_info(para_id)  # XXX: reuse _para?

        for index, (_prefix, _tag, _suffix) in enumerate(paradigm):
            word = _prefix + stem + _suffix
            new_methods_stack = self._fix_stack(methods_stack, word, para_id, index)
            parse = (word, _tag, normal_form, 1.0, new_methods_stack)
            result.append(parse)

        return result

    def normalized(self, form):
        fixed_word, tag, normal_form, score, methods_stack = form
        original_word, para_id, idx = self._extract_para_info(methods_stack)

        if idx == 0:
            return form

        tag = self.dict.build_tag_info(para_id, 0)
        new_methods_stack = self._fix_stack(methods_stack, normal_form, para_id, 0)

        return (normal_form, tag, normal_form, 1.0, new_methods_stack)

    def _extract_para_info(self, methods_stack):
        # This method assumes that DictionaryAnalyzer is the first
        # and the only method in methods_stack.
        analyzer, original_word, para_id, idx = methods_stack[0]
        assert analyzer is self
        return original_word, para_id, idx

    def _fix_stack(self, methods_stack, word, para_id, idx):
        method0 = self, word, para_id, idx
        return (method0,) + methods_stack[1:]
