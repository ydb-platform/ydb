"""
Analogy analyzer units
----------------------

This module provides analyzer units that analyzes unknown words by looking
at how similar known words are analyzed.

"""

import operator

from pymorphy3.dawg import PrefixMatcher
from pymorphy3.units.base import AnalogyAnalyzerUnit
from pymorphy3.units.by_lookup import DictionaryAnalyzer
from pymorphy3.units.utils import (
    add_parse_if_not_seen,
    add_tag_if_not_seen,
    with_prefix,
    without_fixed_prefix
)
from pymorphy3.utils import word_splits

_cnt_getter = operator.itemgetter(3)


class _PrefixAnalyzer(AnalogyAnalyzerUnit):

    def normalizer(self, form, this_method):
        prefix = this_method[1]
        normal_form = yield without_fixed_prefix(form, len(prefix))
        yield with_prefix(normal_form, prefix)

    def lexemizer(self, form, this_method):
        prefix = this_method[1]
        lexeme = yield without_fixed_prefix(form, len(prefix))
        yield [with_prefix(f, prefix) for f in lexeme]


class KnownPrefixAnalyzer(_PrefixAnalyzer):
    """
    Parse the word by checking if it starts with a known prefix
    and parsing the remainder.

    Example: псевдокошка -> (псевдо) + кошка.
    """
    _repr_skip_value_params = ['known_prefixes']

    def __init__(self, known_prefixes, score_multiplier=0.75, min_remainder_length=3):
        self.known_prefixes = known_prefixes
        self.score_multiplier = score_multiplier
        self.min_remainder_length = min_remainder_length

    def init(self, morph):
        super().init(morph)
        self.get_prefixes = PrefixMatcher(self.known_prefixes).prefixes

    def parse(self, word, word_lower, seen_parses):
        result = []
        for prefix, unprefixed_word in self.possible_splits(word_lower):
            method = (self, prefix)

            parses = self.morph.parse(unprefixed_word)
            for fixed_word, tag, normal_form, score, methods_stack in parses:

                if not tag.is_productive():
                    continue

                parse = (
                    prefix + fixed_word,
                    tag,
                    prefix + normal_form,
                    score * self.score_multiplier,
                    methods_stack + (method,)
                )

                add_parse_if_not_seen(parse, result, seen_parses)

        return result

    def tag(self, word, word_lower, seen_tags):
        result = []
        for prefix, unprefixed_word in self.possible_splits(word_lower):
            for tag in self.morph.tag(unprefixed_word):
                if not tag.is_productive():
                    continue
                add_tag_if_not_seen(tag, result, seen_tags)
        return result

    def possible_splits(self, word):
        word_prefixes = self.get_prefixes(word)
        word_prefixes.sort(key=len, reverse=True)
        for prefix in word_prefixes:
            unprefixed_word = word[len(prefix):]

            if len(unprefixed_word) < self.min_remainder_length:
                continue

            yield prefix, unprefixed_word


class UnknownPrefixAnalyzer(_PrefixAnalyzer):
    """
    Parse the word by parsing only the word suffix
    (with restrictions on prefix & suffix lengths).

    Example: байткод -> (байт) + код

    """
    def __init__(self, score_multiplier=0.5):
        self.score_multiplier = score_multiplier

    def init(self, morph):
        super(AnalogyAnalyzerUnit, self).init(morph)
        self.dict_analyzer = DictionaryAnalyzer()
        self.dict_analyzer.init(morph)

    def parse(self, word, word_lower, seen_parses):
        result = []
        for prefix, unprefixed_word in word_splits(word_lower):

            method = (self, prefix)

            parses = self.dict_analyzer.parse(unprefixed_word, unprefixed_word, seen_parses)
            for fixed_word, tag, normal_form, score, methods_stack in parses:

                if not tag.is_productive():
                    continue

                parse = (
                    prefix + fixed_word,
                    tag,
                    prefix + normal_form,
                    score * self.score_multiplier,
                    methods_stack + (method,)
                )
                add_parse_if_not_seen(parse, result, seen_parses)

        return result

    def tag(self, word, word_lower, seen_tags):
        result = []
        for _, unprefixed_word in word_splits(word_lower):

            tags = self.dict_analyzer.tag(unprefixed_word, unprefixed_word, seen_tags)
            for tag in tags:

                if not tag.is_productive():
                    continue

                add_tag_if_not_seen(tag, result, seen_tags)

        return result


class KnownSuffixAnalyzer(AnalogyAnalyzerUnit):
    """
    Parse the word by checking how the words with similar suffixes
    are parsed.

    Example: бутявкать -> ...вкать

    """
    class FakeDictionary(DictionaryAnalyzer):
        """ This is just a DictionaryAnalyzer with different __repr__ """

    def __init__(self, score_multiplier=0.5,  min_word_length=4):
        self.min_word_length = min_word_length
        self.score_multiplier = score_multiplier

    def init(self, morph):
        super().init(morph)
        self._paradigm_prefixes = list(reversed(list(enumerate(self.dict.paradigm_prefixes))))
        self._prediction_splits = list(reversed(range(1, self._max_suffix_length()+1)))

        self.fake_dict = self.FakeDictionary()
        self.fake_dict.init(morph)

    def _max_suffix_length(self):
        try:
            return self.dict.meta['compile_options']['max_suffix_length']
        except KeyError:
            # dicts v2.4 support
            return self.dict.meta['prediction_options']['max_suffix_length']

    def parse(self, word, word_lower, seen_parses):
        result = []
        if len(word) < self.min_word_length:
            return result

        # smoothing; XXX: isn't max_cnt better?
        # or maybe use a proper discounting?
        total_counts = [1] * len(self._paradigm_prefixes)

        for prefix_id, prefix, suffixes_dawg in self._possible_prefixes(word_lower):

            for i in self._prediction_splits:

                # XXX: this should be counted once, not for each prefix
                word_start, word_end = word_lower[:-i], word_lower[-i:]

                para_data = suffixes_dawg.similar_items(word_end, self.morph.char_substitutes)
                for fixed_suffix, parses in para_data:

                    fixed_word = word_start + fixed_suffix

                    for cnt, para_id, idx in parses:
                        tag = self.dict.build_tag_info(para_id, idx)

                        # skip non-productive tags
                        if not tag.is_productive():
                            continue

                        total_counts[prefix_id] += cnt

                        # avoid duplicate parses
                        reduced_parse = fixed_word, tag, para_id
                        if reduced_parse in seen_parses:
                            continue
                        seen_parses.add(reduced_parse)

                        # ok, build the result
                        normal_form = self.dict.build_normal_form(para_id, idx, fixed_word)
                        methods = (
                            (self.fake_dict, fixed_word, para_id, idx),
                            (self, fixed_suffix),
                        )
                        parse = (cnt, fixed_word, tag, normal_form, prefix_id, methods)
                        result.append(parse)

                if total_counts[prefix_id] > 1:
                    break

        result = [
            (fixed_word, tag, normal_form, cnt/total_counts[prefix_id] * self.score_multiplier, methods_stack)
            for (cnt, fixed_word, tag, normal_form, prefix_id, methods_stack) in result
        ]
        result.sort(key=_cnt_getter, reverse=True)
        return result

    def tag(self, word, word_lower, seen_tags):
        # XXX: the result order may be different from
        # ``self.parse(...)``.

        result = []
        if len(word) < self.min_word_length:
            return result

        for prefix_id, prefix, suffixes_dawg in self._possible_prefixes(word_lower):

            for i in self._prediction_splits:

                # XXX: end should be counted once, not for each prefix
                end = word_lower[-i:]

                para_data = suffixes_dawg.similar_items(end, self.morph.char_substitutes)
                found = False

                for fixed_suffix, parses in para_data:
                    for cnt, para_id, idx in parses:

                        tag = self.dict.build_tag_info(para_id, idx)

                        if not tag.is_productive():
                            continue

                        found = True
                        if tag in seen_tags:
                            continue
                        seen_tags.add(tag)
                        result.append((cnt, tag))

                if found:
                    break

        result.sort(reverse=True)
        return [tag for cnt, tag in result]

    def _possible_prefixes(self, word):
        for prefix_id, prefix in self._paradigm_prefixes:
            if not word.startswith(prefix):
                continue

            suffixes_dawg = self.dict.prediction_suffixes_dawgs[prefix_id]
            yield prefix_id, prefix, suffixes_dawg
