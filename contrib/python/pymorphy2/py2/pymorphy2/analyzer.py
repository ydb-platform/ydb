# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals, division
import os
import heapq
import collections
import logging
import threading
import operator
import warnings

from pymorphy2 import opencorpora_dict
from pymorphy2.dawg import ConditionalProbDistDAWG
import pymorphy2.lang

logger = logging.getLogger(__name__)

_Parse = collections.namedtuple('Parse', 'word, tag, normal_form, score, methods_stack')

_score_getter = operator.itemgetter(3)
auto = object()


class Parse(_Parse):
    """
    Parse result wrapper.
    """

    _morph = None
    """ :type _morph: MorphAnalyzer """

    _dict = None
    """ :type _dict: pymorphy2.opencorpora_dict.Dictionary """

    def inflect(self, required_grammemes):
        res = self._morph._inflect(self, required_grammemes)
        return None if not res else res[0]

    def make_agree_with_number(self, num):
        """
        Inflect the word so that it agrees with ``num``
        """
        return self.inflect(self.tag.numeral_agreement_grammemes(num))

    @property
    def lexeme(self):
        """ A lexeme this form belongs to. """
        return self._morph.get_lexeme(self)

    @property
    def is_known(self):
        """ True if this form is a known dictionary form. """
        return self._dict.word_is_known(
            word=self.word,
            substitutes_compiled=self._morph.char_substitutes
        )

    @property
    def normalized(self):
        """ A :class:`Parse` instance for :attr:`self.normal_form`. """
        last_method = self.methods_stack[-1]
        return self.__class__(*last_method[0].normalized(self))

    # @property
    # def paradigm(self):
    #     return self._dict.build_paradigm_info(self.para_id)


class ProbabilityEstimator(object):
    def __init__(self, dict_path):
        cpd_path = os.path.join(dict_path, 'p_t_given_w.intdawg')
        self.p_t_given_w = ConditionalProbDistDAWG().load(cpd_path)

    def apply_to_parses(self, word, word_lower, parses):
        if not parses:
            return parses

        probs = [self.p_t_given_w.prob(word_lower, tag)
                for (word, tag, normal_form, score, methods_stack) in parses]

        if sum(probs) == 0:
            # no P(t|w) information is available; return normalized estimate
            k = 1.0 / sum(map(_score_getter, parses))
            return [
                (word, tag, normal_form, score*k, methods_stack)
                for (word, tag, normal_form, score, methods_stack) in parses
            ]

        # replace score with P(t|w) probability
        return sorted([
            (word, tag, normal_form, prob, methods_stack)
            for (word, tag, normal_form, score, methods_stack), prob
            in zip(parses, probs)
        ], key=_score_getter, reverse=True)

    def apply_to_tags(self, word, word_lower, tags):
        if not tags:
            return tags
        return sorted(tags,
            key=lambda tag: self.p_t_given_w.prob(word_lower, tag),
            reverse=True
        )


def _iter_entry_points(*args, **kwargs):
    """ Like pkg_resources.iter_entry_points, but uses a WorkingSet which
    is not populated at startup. This ensures that all entry points
    are picked up, even if a package which provides them is installed
    after the current process is started.

    The main use case is to make ``!pip install pymorphy2`` work
    within a Jupyter or Google Colab notebook.
    See https://github.com/kmike/pymorphy2/issues/131
    """
    import pkg_resources
    ws = pkg_resources.WorkingSet()
    return ws.iter_entry_points(*args, **kwargs)


def _lang_dict_paths():
    paths = dict(
        (pkg.name, pkg.load().get_path())
        for pkg in _iter_entry_points('pymorphy2_dicts')
    )

    # discovery of pymorphy2 v0.8 dicts
    try:
        import pymorphy2_dicts
        paths['ru-old'] = pymorphy2_dicts.get_path()
    except ImportError:
        pass

    return paths


def lang_dict_path(lang):
    """ Return language-specific dictionary path """
    lang_paths = _lang_dict_paths()
    if lang in lang_paths:
        return lang_paths[lang]

    raise ValueError(
        "Can't find a dictionary for language %r. Installed languages: %r. "
        "Try installing pymorphy2-dicts-%s package." % (
            lang, list(lang_paths.keys()), lang
        )
    )


class MorphAnalyzer(object):
    """
    Morphological analyzer for Russian language.

    For a given word it can find all possible inflectional paradigms
    and thus compute all possible tags and normal forms.

    Analyzer uses morphological word features and a lexicon
    (dictionary compiled from XML available at OpenCorpora.org);
    for unknown words heuristic algorithm is used.

    Create a :class:`MorphAnalyzer` object::

        >>> import pymorphy2
        >>> morph = pymorphy2.MorphAnalyzer()

    MorphAnalyzer uses dictionaries from ``pymorphy2-dicts`` package
    (which can be installed via ``pip install pymorphy2-dicts``).

    Alternatively (e.g. if you have your own precompiled dictionaries),
    either create ``PYMORPHY2_DICT_PATH`` environment variable
    with a path to dictionaries, or pass ``path`` argument
    to :class:`pymorphy2.MorphAnalyzer` constructor::

        >>> morph = pymorphy2.MorphAnalyzer(path='/path/to/dictionaries') # doctest: +SKIP

    By default, methods of this class return parsing results
    as namedtuples :class:`Parse`. This has performance implications
    under CPython, so if you need maximum speed then pass
    ``result_type=None`` to make analyzer return plain unwrapped tuples::

        >>> morph = pymorphy2.MorphAnalyzer(result_type=None)

    """
    DICT_PATH_ENV_VARIABLE = 'PYMORPHY2_DICT_PATH'
    DEFAULT_UNITS = pymorphy2.lang.ru.DEFAULT_UNITS
    DEFAULT_SUBSTITUTES = pymorphy2.lang.ru.CHAR_SUBSTITUTES
    char_substitutes = None

    _lock = threading.RLock()

    def __init__(self, path=None, lang=None, result_type=Parse, units=None,
                 probability_estimator_cls=auto, char_substitutes=auto):

        # save arguments for pickling/unpickling
        self._path = path
        self._lang = lang

        if path is None and lang is None:
            lang = 'ru'

        path = self.choose_dictionary_path(path, lang)

        with self._lock:
            self.dictionary = opencorpora_dict.Dictionary(path)
            self.lang = self.choose_language(self.dictionary, lang)

            self.prob_estimator = self._get_prob_estimator(
                probability_estimator_cls, self.dictionary, path
            )

            if result_type is not None:
                # create a subclass with the same name,
                # but with _morph attribute bound to self
                res_type = type(
                    result_type.__name__,
                    (result_type,),
                    {'_morph': self, '_dict': self.dictionary}
                )
                self._result_type = res_type
            else:
                self._result_type = None

            self._result_type_orig = result_type
            self._init_char_substitutes(char_substitutes)
            self._init_units(units)

    def _init_units(self, units_unbound=None):
        if units_unbound is None:
            units_unbound = self._config_value('DEFAULT_UNITS', self.DEFAULT_UNITS)

        self._units_unbound = units_unbound
        self._units = []
        for item in units_unbound:
            if isinstance(item, (list, tuple)):
                for unit in item[:-1]:
                    self._units.append((self._bound_unit(unit), False))
                self._units.append((self._bound_unit(item[-1]), True))
            else:
                self._units.append((self._bound_unit(item), True))

    def _init_char_substitutes(self, char_substitutes):
        if char_substitutes is auto:
            char_substitutes = self._config_value('CHAR_SUBSTITUTES', self.DEFAULT_SUBSTITUTES)
        self.char_substitutes = self.dictionary.words.compile_replaces(char_substitutes or {})

    def _bound_unit(self, unit):
        unit = unit.clone()
        unit.init(self)
        return unit

    def _lang_default_config(self):
        assert self.lang is not None
        aliases = {'ru-old': 'ru'}
        lang = aliases.get(self.lang, self.lang)
        if not hasattr(pymorphy2.lang, lang):
            warnings.warn("unknown language code: %r" % lang)
            return None
        return getattr(pymorphy2.lang, lang)

    def _config_value(self, key, default):
        config = self._lang_default_config()
        return getattr(config, key, default)

    @classmethod
    def _get_prob_estimator(cls, estimator_cls, dictionary, path):
        if estimator_cls is auto:
            if dictionary.meta.get('P(t|w)'):
                estimator_cls = ProbabilityEstimator
        if estimator_cls is auto or estimator_cls is None:
            return None
        return estimator_cls(path)

    @classmethod
    def choose_dictionary_path(cls, path=None, lang=None):
        if path is not None:
            return path

        if cls.DICT_PATH_ENV_VARIABLE in os.environ:
            return os.environ[cls.DICT_PATH_ENV_VARIABLE]

        return lang_dict_path(lang)

    @classmethod
    def choose_language(cls, dictionary, lang):
        if lang is None:
            if dictionary.lang is None:
                # this could be e.g. old pymorphy2 dictionary
                warnings.warn("Dictionary doesn't declare its language; "
                              "assuming 'ru'")
                return 'ru'
            return dictionary.lang

        if dictionary.lang != lang:
            # allow incorrect 'lang' values, but show a warning
            warnings.warn(
                "Dictionary language (%r) doesn't match "
                "analyzer language (%r)." % (dictionary.lang, lang)
            )

        return lang

    def parse(self, word):
        """
        Analyze the word and return a list of :class:`pymorphy2.analyzer.Parse`
        namedtuples:

            Parse(word, tag, normal_form, para_id, idx, _score)

        (or plain tuples if ``result_type=None`` was used in constructor).
        """
        res = []
        seen = set()
        word_lower = word.lower()

        for analyzer, is_terminal in self._units:
            res.extend(analyzer.parse(word, word_lower, seen))

            if is_terminal and res:
                break

        if self.prob_estimator is not None:
            res = self.prob_estimator.apply_to_parses(word, word_lower, res)

        if self._result_type is None:
            return res

        return [self._result_type(*p) for p in res]

    def tag(self, word):
        res = []
        seen = set()
        word_lower = word.lower()

        for analyzer, is_terminal in self._units:
            res.extend(analyzer.tag(word, word_lower, seen))

            if is_terminal and res:
                break

        if self.prob_estimator is not None:
            res = self.prob_estimator.apply_to_tags(word, word_lower, res)
        return res

    def normal_forms(self, word):
        """
        Return a list of word normal forms.
        """
        seen = set()
        result = []

        for p in self.parse(word):
            normal_form = p[2]
            if normal_form not in seen:
                result.append(normal_form)
                seen.add(normal_form)
        return result

    # ==== inflection ========

    def get_lexeme(self, form):
        """
        Return the lexeme this parse belongs to.
        """
        methods_stack = form[4]
        last_method = methods_stack[-1]
        result = last_method[0].get_lexeme(form)

        if self._result_type is None:
            return result
        return [self._result_type(*p) for p in result]

    def _inflect(self, form, required_grammemes):
        possible_results = [f for f in self.get_lexeme(form)
                            if required_grammemes <= f[1].grammemes]

        if not possible_results:
            required_grammemes = self.TagClass.fix_rare_cases(required_grammemes)
            possible_results = [f for f in self.get_lexeme(form)
                                if required_grammemes <= f[1].grammemes]

        grammemes = form[1].updated_grammemes(required_grammemes)
        def similarity(frm):
            tag = frm[1]
            return len(grammemes & tag.grammemes) - 0.1 * len(grammemes ^ tag.grammemes)

        return heapq.nlargest(1, possible_results, key=similarity)

    # ====== misc =========

    def iter_known_word_parses(self, prefix=""):
        """
        Return an iterator over parses of dictionary words that starts
        with a given prefix (default empty prefix means "all words").
        """

        # XXX: this method currently assumes that
        # units.DictionaryAnalyzer is the first analyzer unit.
        for word, tag, normal_form, para_id, idx in self.dictionary.iter_known_words(prefix):
            methods = ((self._units[0][0], word, para_id, idx),)
            parse = (word, tag, normal_form, 1.0, methods)
            if self._result_type is None:
                yield parse
            else:
                yield self._result_type(*parse)

    def word_is_known(self, word, strict=False):
        """
        Check if a ``word`` is in the dictionary.

        By default, some fuzziness is allowed, depending on a
        dictionary - e.g. for Russian ё letters replaced with е are handled.
        Pass ``strict=True`` to make matching strict (e.g. if it is
        guaranteed the ``word`` has correct е/ё or г/ґ letters).

        .. note::

            Dictionary words are not always correct words;
            the dictionary also contains incorrect forms which
            are commonly used. So for spellchecking tasks this
            method should be used with extra care.

        """
        return self.dictionary.word_is_known(
            word = word.lower(),
            substitutes_compiled = None if strict else self.char_substitutes
        )

    @property
    def TagClass(self):
        """
        :rtype: pymorphy2.tagset.OpencorporaTag
        """
        return self.dictionary.Tag

    def cyr2lat(self, tag_or_grammeme):
        """ Return Latin representation for ``tag_or_grammeme`` string """
        return self.TagClass.cyr2lat(tag_or_grammeme)

    def lat2cyr(self, tag_or_grammeme):
        """ Return Cyrillic representation for ``tag_or_grammeme`` string """
        return self.TagClass.lat2cyr(tag_or_grammeme)

    def __reduce__(self):
        args = (self._path, self._lang, self._result_type_orig, self._units_unbound)
        return self.__class__, args, None
