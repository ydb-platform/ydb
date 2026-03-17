# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, division
import logging
from .storage import load_dict

logger = logging.getLogger(__name__)


class Dictionary(object):
    """
    OpenCorpora dictionary wrapper class.
    """

    def __init__(self, path):

        logger.info("Loading dictionaries from %s", path)

        self._data = load_dict(path)

        logger.info("format: %(format_version)s, revision: %(source_revision)s, updated: %(compiled_at)s", self._data.meta)

        # attributes from opencorpora_dict.storage.LoadedDictionary
        self.paradigms = self._data.paradigms
        self.gramtab = self._data.gramtab
        self.paradigm_prefixes = self._data.paradigm_prefixes
        self.suffixes = self._data.suffixes
        self.words = self._data.words
        self.prediction_suffixes_dawgs = self._data.prediction_suffixes_dawgs
        self.meta = self._data.meta
        self.Tag = self._data.Tag
        self.lang = self.meta.get('language_code')

        # extra attributes
        self.path = path

    def build_tag_info(self, para_id, idx):
        """
        Return tag as a string.
        """
        paradigm = self.paradigms[para_id]
        tag_info_offset = len(paradigm) // 3
        tag_id = paradigm[tag_info_offset + idx]
        return self.gramtab[tag_id]

    def build_paradigm_info(self, para_id):
        """
        Return a list of

            (prefix, tag, suffix)

        tuples representing the paradigm.
        """
        paradigm = self.paradigms[para_id]
        paradigm_len = len(paradigm) // 3
        res = []
        for idx in range(paradigm_len):
            prefix_id = paradigm[paradigm_len*2 + idx]
            prefix = self.paradigm_prefixes[prefix_id]

            suffix_id = paradigm[idx]
            suffix = self.suffixes[suffix_id]

            res.append(
                (prefix, self.build_tag_info(para_id, idx), suffix)
            )
        return res

    def build_normal_form(self, para_id, idx, fixed_word):
        """
        Build a normal form.
        """

        if idx == 0:  # a shortcut: normal form is a word itself
            return fixed_word

        paradigm = self.paradigms[para_id]
        paradigm_len = len(paradigm) // 3

        stem = self.build_stem(paradigm, idx, fixed_word)

        normal_prefix_id = paradigm[paradigm_len*2 + 0]
        normal_suffix_id = paradigm[0]

        normal_prefix = self.paradigm_prefixes[normal_prefix_id]
        normal_suffix = self.suffixes[normal_suffix_id]

        return normal_prefix + stem + normal_suffix

    def build_stem(self, paradigm, idx, fixed_word):
        """
        Return word stem (given a word, paradigm and the word index).
        """
        paradigm_len = len(paradigm) // 3

        prefix_id = paradigm[paradigm_len*2 + idx]
        prefix = self.paradigm_prefixes[prefix_id]

        suffix_id = paradigm[idx]
        suffix = self.suffixes[suffix_id]

        if suffix:
            return fixed_word[len(prefix):-len(suffix)]
        else:
            return fixed_word[len(prefix):]

    def word_is_known(self, word, substitutes_compiled=None):
        """
        Check if a ``word`` is in the dictionary.

        To allow some fuzzyness pass ``substitutes_compiled`` argument;
        it should be a result of :meth:`DAWG.compile_replaces()`.
        This way you can e.g. handle ё letters replaced with е in the
        input words.

        .. note::

            Dictionary words are not always correct words;
            the dictionary also contains incorrect forms which
            are commonly used. So for spellchecking tasks this
            method should be used with extra care.

        """
        if substitutes_compiled:
            return bool(self.words.similar_keys(word, substitutes_compiled))
        else:
            return word in self.words

    def iter_known_words(self, prefix=""):
        """
        Return an iterator over ``(word, tag, normal_form, para_id, idx)``
        tuples with dictionary words that starts with a given prefix
        (default empty prefix means "all words").
        """

        for word, (para_id, idx) in self.words.iteritems(prefix):
            tag = self.build_tag_info(para_id, idx)
            normal_form = self.build_normal_form(para_id, idx, word)
            yield word, tag, normal_form, para_id, idx

    def __repr__(self):
        return str("<%s>") % self.__class__.__name__


