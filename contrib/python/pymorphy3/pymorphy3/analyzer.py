import heapq
import logging
import operator
import os
import threading
import warnings
from typing import NamedTuple, Union, Set, List, Type

import pymorphy3.lang
from pymorphy3 import opencorpora_dict
from pymorphy3 import tagset
from pymorphy3.dawg import ConditionalProbDistDAWG
from pymorphy3.units.base import BaseAnalyzerUnit

logger = logging.getLogger(__name__)
_score_getter = operator.itemgetter(3)
auto = object()


class _Parse(NamedTuple):
    word: str
    tag: tagset.OpencorporaTag
    normal_form: str
    score: float
    methods_stack: tuple


class Parse(_Parse):
    """
    Parse result wrapper.
    """

    _morph: "MorphAnalyzer" = None
    _dict: opencorpora_dict.Dictionary = None

    def inflect(self, required_grammemes: Set[str]) -> Union["Parse", None]:
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


class ProbabilityEstimator:
    def __init__(self, dict_path):
        cpd_path = os.path.join(dict_path, 'p_t_given_w.intdawg')
        self.p_t_given_w = ConditionalProbDistDAWG().load(cpd_path)

    def apply_to_parses(self, word: str, word_lower: str, parses: List[Parse]) -> List[_Parse]:
        if not parses:
            return []

        probs = [
            self.p_t_given_w.prob(word_lower, tag)
            for (word, tag, normal_form, score, methods_stack) in parses
        ]

        if sum(probs) == 0:
            # no P(t|w) information is available; return normalized estimate
            k = 1.0 / sum(map(_score_getter, parses))
            return [
                _Parse(word, tag, normal_form, score * k, methods_stack)
                for (word, tag, normal_form, score, methods_stack) in parses
            ]

        # replace score with P(t|w) probability
        return sorted([
            _Parse(word, tag, normal_form, prob, methods_stack)
            for (word, tag, normal_form, score, methods_stack), prob
            in zip(parses, probs)
        ], key=_score_getter, reverse=True)

    def apply_to_tags(self, word_lower: str, tags: List[tagset.OpencorporaTag]) -> List[tagset.OpencorporaTag]:
        if not tags:
            return tags
        return sorted(tags,
            key=lambda tag: self.p_t_given_w.prob(word_lower, tag),
            reverse=True
        )


def _iter_entry_points(*args):
    """
    Finds entry points of installed packages, including ones installed
    after the current process is started.

    The main use case is to make ``!pip install pymorphy3`` work
    within a Jupyter or Google Colab notebook.
    See https://github.com/kmike/pymorphy2/issues/131
    """
    from importlib.metadata import entry_points, EntryPoint
    ep = entry_points()
    result: set[EntryPoint] = set()
    if not hasattr(ep, 'select'):
        # Python < 3.10 old entry_points API
        for group in args:
            result.update(ep.get(group, ()))
    else:
        for group in args:
            result.update(ep.select(group=group))
    return result


def _lang_dict_paths():
    paths = dict(
        (pkg.name, pkg.load().get_path())
        for pkg in _iter_entry_points('pymorphy3_dicts')
    )

    # discovery of pymorphy3 v0.8 dicts
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
        f"Can't find a dictionary for language {repr(lang)}. Installed languages: {repr(list(lang_paths.keys()))}. "
        f"Try installing pymorphy3-dicts-{lang} package."
    )


class _Unit(NamedTuple):
    analyzer: BaseAnalyzerUnit
    is_terminal: bool


class MorphAnalyzer:
    """
    Morphological analyzer for Russian language.

    For a given word it can find all possible inflectional paradigms
    and thus compute all possible tags and normal forms.

    Analyzer uses morphological word features and a lexicon
    (dictionary compiled from XML available at OpenCorpora.org);
    for unknown words heuristic algorithm is used.

    Create a :class:`MorphAnalyzer` object::

        >>> import pymorphy3
        >>> morph = pymorphy3.MorphAnalyzer()

    MorphAnalyzer uses dictionaries from ``pymorphy2-dicts`` package
    (which can be installed via ``pip install pymorphy2-dicts``).

    Alternatively (e.g. if you have your own precompiled dictionaries),
    either create ``PYMORPHY2_DICT_PATH`` environment variable
    with a path to dictionaries, or pass ``path`` argument
    to :class:`pymorphy3.MorphAnalyzer` constructor::

        >>> morph = pymorphy3.MorphAnalyzer(path='/path/to/dictionaries') # doctest: +SKIP

    By default, methods of this class return parsing results
    as namedtuples :class:`Parse`. This has performance implications
    under CPython, so if you need maximum speed then pass
    ``result_type=None`` to make analyzer return plain unwrapped tuples::

        >>> morph = pymorphy3.MorphAnalyzer(result_type=None)

    """
    DICT_PATH_ENV_VARIABLE = 'PYMORPHY2_DICT_PATH'
    DEFAULT_UNITS = pymorphy3.lang.ru.DEFAULT_UNITS
    DEFAULT_SUBSTITUTES = pymorphy3.lang.ru.CHAR_SUBSTITUTES
    char_substitutes = None

    _lock = threading.RLock()
    _units: List[_Unit]
    _result_type: Union[Type[Parse], None]

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
                    self._units.append(_Unit(self._bound_unit(unit), False))
                self._units.append(_Unit(self._bound_unit(item[-1]), True))
            else:
                self._units.append(_Unit(self._bound_unit(item), True))

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
        if not hasattr(pymorphy3.lang, lang):
            warnings.warn(f"unknown language code: {repr(lang)}")
            return None
        return getattr(pymorphy3.lang, lang)

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
    def choose_language(cls, dictionary: opencorpora_dict.Dictionary, lang: Union[str, None]) -> str:
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
                f"Dictionary language ({repr(dictionary.lang)}) doesn't match "
                f"analyzer language ({repr(lang)})."
            )

        return lang

    def parse(self, word: str) -> List[Parse]:
        """
        Analyze the word and return a list of :class:`pymorphy3.analyzer.Parse`
        namedtuples:

            Parse(word, tag, normal_form, para_id, idx, _score)

        (or plain tuples if ``result_type=None`` was used in constructor).
        """
        res: List[Parse] = []
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

    def tag(self, word: str) -> List[tagset.OpencorporaTag]:
        res = []
        seen = set()
        word_lower = word.lower()

        for analyzer, is_terminal in self._units:
            res.extend(analyzer.tag(word, word_lower, seen))

            if is_terminal and res:
                break

        if self.prob_estimator is not None:
            res = self.prob_estimator.apply_to_tags(word_lower, res)
        return res

    def normal_forms(self, word: str) -> List[str]:
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

    def get_lexeme(self, form: Parse):
        """
        Return the lexeme this parse belongs to.
        """
        methods_stack = form.methods_stack
        last_method = methods_stack[-1]
        result = last_method[0].get_lexeme(form)

        if self._result_type is None:
            return result
        return [self._result_type(*p) for p in result]

    def _inflect(self, form: Parse, required_grammemes: Set[str]):
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

    def word_is_known(self, word: str, strict: bool = False) -> bool:
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
    def TagClass(self) -> tagset.OpencorporaTag:
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
