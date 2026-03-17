# -*- coding: utf-8 -*-
"""
Utils for working with grammatical tags.
"""
from __future__ import absolute_import, unicode_literals
import collections

try:
    from sys import intern
except ImportError:
    # python 2.x has builtin ``intern`` function
    pass

# a bit of *heavy* magic...
class _select_grammeme_from(object):
    """
    Descriptor object for accessing grammemes of certain classes
    (e.g. number or voice).
    """
    def __init__(self, grammeme_set):
        self.grammeme_set = grammeme_set
        # ... are descriptors not magical enough?

        # In order to fight typos, raise an exception
        # if a result is compared to a grammeme which
        # is not in a set of allowed grammemes.
        _str = type("unicode string")

        class TypedGrammeme(_str):
            def __eq__(self, other):
                if other is None:
                    return False
                if other not in grammeme_set:
                    known_grammemes = ", ".join(grammeme_set)
                    raise ValueError("'%s' is not a valid grammeme for this attribute. Valid grammemes: %s" % (other, known_grammemes))
                return _str.__eq__(self, other)

            def __ne__(self, other):
                return not self.__eq__(other)

            def __hash__(self):
                return _str.__hash__(self)

        self.TypedGrammeme = TypedGrammeme

    def __get__(self, instance, owner):
        grammemes = self.grammeme_set & instance.grammemes

        if not grammemes:
            # XXX: type checks are not enforced in this case
            return None

        res = next(iter(grammemes))
        return self.TypedGrammeme(res) if owner.typed_grammemes else res


# Design notes: Tag objects are immutable, but the tag class is mutable.
class OpencorporaTag(object):
    """
    Wrapper class for OpenCorpora.org tags.

    .. warning::

        In order to work properly, the class has to be globally
        initialized with actual grammemes (using _init_grammemes method).

        Pymorphy2 initializes it when loading a dictionary;
        it may be not a good idea to use this class directly.
        If possible, use ``morph_analyzer.TagClass`` instead.

    Example::

        >>> from pymorphy2 import MorphAnalyzer
        >>> morph = MorphAnalyzer()
        >>> Tag = morph.TagClass  # get an initialzed Tag class
        >>> tag = Tag('VERB,perf,tran plur,impr,excl')
        >>> tag
        OpencorporaTag('VERB,perf,tran plur,impr,excl')

    Tag instances have attributes for accessing grammemes::

        >>> print(tag.POS)
        VERB
        >>> print(tag.number)
        plur
        >>> print(tag.case)
        None

    Available attributes are: POS, animacy, aspect, case, gender, involvement,
    mood, number, person, tense, transitivity and voice.

    You may check if a grammeme is in tag or if all grammemes
    from a given set are in tag::

        >>> 'perf' in tag
        True
        >>> 'nomn' in tag
        False
        >>> 'Geox' in tag
        False
        >>> set(['VERB', 'perf']) in tag
        True
        >>> set(['VERB', 'perf', 'sing']) in tag
        False

    In order to fight typos, for unknown grammemes an exception is raised::

        >>> 'foobar' in tag
        Traceback (most recent call last):
        ...
        ValueError: Grammeme is unknown: foobar
        >>> set(['NOUN', 'foo', 'bar']) in tag
        Traceback (most recent call last):
        ...
        ValueError: Grammemes are unknown: {'bar', 'foo'}

    This also works for attributes::

        >>> tag.POS == 'plur'
        Traceback (most recent call last):
        ...
        ValueError: 'plur' is not a valid grammeme for this attribute. Valid grammemes: ...

    """

    # Grammeme categories
    # (see http://opencorpora.org/dict.php?act=gram for a full set)
    # -------------------------------------------------------------

    PARTS_OF_SPEECH = frozenset([
        'NOUN',  # имя существительное
        'ADJF',  # имя прилагательное (полное)
        'ADJS',  # имя прилагательное (краткое)
        'COMP',  # компаратив
        'VERB',  # глагол (личная форма)
        'INFN',  # глагол (инфинитив)
        'PRTF',  # причастие (полное)
        'PRTS',  # причастие (краткое)
        'GRND',  # деепричастие
        'NUMR',  # числительное
        'ADVB',  # наречие
        'NPRO',  # местоимение-существительное
        'PRED',  # предикатив
        'PREP',  # предлог
        'CONJ',  # союз
        'PRCL',  # частица
        'INTJ',  # междометие
    ])

    ANIMACY = frozenset([
        'anim',  # одушевлённое
        'inan',  # неодушевлённое
    ])

    GENDERS = frozenset([
        'masc',  # мужской род
        'femn',  # женский род
        'neut',  # средний род
    ])

    NUMBERS = frozenset([
        'sing',  # единственное число
        'plur',  # множественное число
    ])

    CASES = frozenset([
        'nomn',  # именительный падеж
        'gent',  # родительный падеж
        'datv',  # дательный падеж
        'accs',  # винительный падеж
        'ablt',  # творительный падеж
        'loct',  # предложный падеж
        'voct',  # звательный падеж
        'gen1',  # первый родительный падеж
        'gen2',  # второй родительный (частичный) падеж
        'acc2',  # второй винительный падеж
        'loc1',  # первый предложный падеж
        'loc2',  # второй предложный (местный) падеж
    ])

    ASPECTS = frozenset([
        'perf',  # совершенный вид
        'impf',  # несовершенный вид
    ])

    TRANSITIVITY = frozenset([
        'tran',  # переходный
        'intr',  # непереходный
    ])

    PERSONS = frozenset([
        '1per',  # 1 лицо
        '2per',  # 2 лицо
        '3per',  # 3 лицо
    ])

    TENSES = frozenset([
        'pres',  # настоящее время
        'past',  # прошедшее время
        'futr',  # будущее время
    ])

    MOODS = frozenset([
        'indc',  # изъявительное наклонение
        'impr',  # повелительное наклонение
    ])

    VOICES = frozenset([
        'actv',  # действительный залог
        'pssv',  # страдательный залог
    ])

    INVOLVEMENT = frozenset([
        'incl',  # говорящий включён в действие
        'excl',  # говорящий не включён в действие
    ])

    # Set this to False (as a class attribute) to disable strict
    # grammeme type checking for tag.POS, tag.voice, etc. attributes.
    # Without type checks comparisons are about 2x faster.
    typed_grammemes = True

    # Tag format identifier
    # (compatible with https://github.com/kmike/russian-tagsets)
    # ----------------------------------------------------------
    FORMAT = 'opencorpora-int'


    # Helper attributes for inflection/declension routines
    # ----------------------------------------------------
    _NON_PRODUCTIVE_GRAMMEMES = set(['NUMR', 'NPRO', 'PRED', 'PREP',
                                     'CONJ', 'PRCL', 'INTJ', 'Apro'])
    _EXTRA_INCOMPATIBLE = {  # XXX: is it a good idea to have these rules?
        'plur': set(['GNdr']),
        # XXX: how to use rules from OpenCorpora?
        # (they have "lexeme/form" separation)
    }
    _GRAMMEME_INDICES = collections.defaultdict(int)
    _GRAMMEME_INCOMPATIBLE = collections.defaultdict(set)
    _LAT2CYR = None
    _CYR2LAT = None
    KNOWN_GRAMMEMES = set()

    _NUMERAL_AGREEMENT_GRAMMEMES = (
        set(['sing', 'nomn']),
        set(['sing', 'accs']),
        set(['sing', 'gent']),
        set(['plur', 'nomn']),
        set(['plur', 'gent']),
    )

    RARE_CASES = {
        'gen1': 'gent',
        'gen2': 'gent',
        'acc1': 'accs',
        'acc2': 'accs',
        'loc1': 'loct',
        'loc2': 'loct',
        'voct': 'nomn'
    }

    __slots__ = ['_grammemes_tuple', '_grammemes_cache', '_str', '_POS',
                 '_cyr', '_cyr_grammemes_cache']

    def __init__(self, tag):
        self._str = tag
        # XXX: we loose information about which grammemes
        # belongs to lexeme and which belongs to form
        # (but this information seems useless for pymorphy2).

        # Hacks for better memory usage:
        # - store grammemes in a tuple and build a set only when needed;
        # - use byte strings for grammemes under Python 2.x;
        # - grammemes are interned.
        grammemes = tag.replace(' ', ',', 1).split(',')
        grammemes_tuple = tuple([intern(str(g)) for g in grammemes])

        self._assert_grammemes_are_known(set(grammemes_tuple))

        self._grammemes_tuple = grammemes_tuple
        self._POS = self._grammemes_tuple[0]
        self._grammemes_cache = None
        self._cyr_grammemes_cache = None
        self._cyr = None

    # attributes for grammeme categories
    POS = _select_grammeme_from(PARTS_OF_SPEECH)
    animacy = _select_grammeme_from(ANIMACY)
    aspect = _select_grammeme_from(ASPECTS)
    case = _select_grammeme_from(CASES)
    gender = _select_grammeme_from(GENDERS)
    involvement = _select_grammeme_from(INVOLVEMENT)
    mood = _select_grammeme_from(MOODS)
    number = _select_grammeme_from(NUMBERS)
    person = _select_grammeme_from(PERSONS)
    tense = _select_grammeme_from(TENSES)
    transitivity = _select_grammeme_from(TRANSITIVITY)
    voice = _select_grammeme_from(VOICES)

    @property
    def grammemes(self):
        """ A frozenset with grammemes for this tag. """
        if self._grammemes_cache is None:
            self._grammemes_cache = frozenset(self._grammemes_tuple)
        return self._grammemes_cache

    @property
    def grammemes_cyr(self):
        """ A frozenset with Cyrillic grammemes for this tag. """
        if self._cyr_grammemes_cache is None:
            cyr_grammemes = [self._LAT2CYR[g] for g in self._grammemes_tuple]
            self._cyr_grammemes_cache = frozenset(cyr_grammemes)
        return self._cyr_grammemes_cache

    @property
    def cyr_repr(self):
        """ Cyrillic representation of this tag """
        if self._cyr is None:
            self._cyr = self.lat2cyr(self)
        return self._cyr

    @classmethod
    def cyr2lat(cls, tag_or_grammeme):
        """ Return Latin representation for ``tag_or_grammeme`` string """
        return _translate_tag(tag_or_grammeme, cls._CYR2LAT)

    @classmethod
    def lat2cyr(cls, tag_or_grammeme):
        """ Return Cyrillic representation for ``tag_or_grammeme`` string """
        return _translate_tag(tag_or_grammeme, cls._LAT2CYR)

    def __contains__(self, grammeme):

        # {'NOUN', 'sing'} in tag
        if isinstance(grammeme, (set, frozenset)):
            if grammeme <= self.grammemes:
                return True
            self._assert_grammemes_are_known(grammeme)
            return False

        # 'NOUN' in tag
        if grammeme in self.grammemes:
            return True
        else:
            if not self.grammeme_is_known(grammeme):
                raise ValueError("Grammeme is unknown: %s" % grammeme)
            return False

    # FIXME: __repr__ and __str__ always return unicode,
    # but they should return a byte string under Python 2.x.
    def __str__(self):
        return self._str

    def __repr__(self):
        return "OpencorporaTag('%s')" % self


    def __eq__(self, other):
        return self._grammemes_tuple == other._grammemes_tuple

    def __ne__(self, other):
        return self._grammemes_tuple != other._grammemes_tuple

    def __lt__(self, other):
        return self._grammemes_tuple < other._grammemes_tuple

    def __gt__(self, other):
        return self._grammemes_tuple > other._grammemes_tuple

    def __hash__(self):
        return hash(self._grammemes_tuple)

    def __len__(self):
        return len(self._grammemes_tuple)

    def __reduce__(self):
        return self.__class__, (self._str,), None


    def is_productive(self):
        return not self.grammemes & self._NON_PRODUCTIVE_GRAMMEMES

    def _is_unknown(self):
        return self._POS not in self.PARTS_OF_SPEECH

    @classmethod
    def grammeme_is_known(cls, grammeme):
        cls._assert_grammemes_initialized()
        return grammeme in cls.KNOWN_GRAMMEMES

    @classmethod
    def _assert_grammemes_are_known(cls, grammemes):
        if not grammemes <= cls.KNOWN_GRAMMEMES:
            cls._assert_grammemes_initialized()
            unknown = grammemes - cls.KNOWN_GRAMMEMES
            unknown_repr = ", ".join(["'%s'" % g for g in sorted(unknown)])
            raise ValueError("Grammemes are unknown: {%s}" % unknown_repr)

    @classmethod
    def _assert_grammemes_initialized(cls):
        if not cls.KNOWN_GRAMMEMES:
            msg = "The class was not properly initialized."
            raise RuntimeError(msg)

    def updated_grammemes(self, required):
        """
        Return a new set of grammemes with ``required`` grammemes added
        and incompatible grammemes removed.
        """
        new_grammemes = self.grammemes | required
        for grammeme in required:
            if not self.grammeme_is_known(grammeme):
                raise ValueError("Unknown grammeme: %s" % grammeme)
            new_grammemes -= self._GRAMMEME_INCOMPATIBLE[grammeme]
        return new_grammemes

    @classmethod
    def fix_rare_cases(cls, grammemes):
        """
        Replace rare cases (loc2/voct/...) with common ones (loct/nomn/...).
        """
        return frozenset(cls.RARE_CASES.get(g, g) for g in grammemes)

    @classmethod
    def add_grammemes_to_known(cls, lat, cyr, overwrite=True):
        if not overwrite and lat in cls.KNOWN_GRAMMEMES:
            return
        cls.KNOWN_GRAMMEMES.add(lat)
        cls._LAT2CYR[lat] = cyr
        cls._CYR2LAT[cyr] = lat

    @classmethod
    def _init_grammemes(cls, dict_grammemes):
        """
        Initialize various class attributes with grammeme
        information obtained from XML dictionary.

        ``dict_grammemes`` is a list of tuples::

            [
                (name, parent, alias, description),
                ...
            ]

        """
        cls.KNOWN_GRAMMEMES = set()
        cls._CYR2LAT = {}
        cls._LAT2CYR = {}
        for name, parent, alias, description in dict_grammemes:
            cls.add_grammemes_to_known(name, alias)

        gr = dict((name, parent) for (name, parent, alias, description) in dict_grammemes)

        # figure out parents & children
        children = collections.defaultdict(set)
        for index, (name, parent, alias, description) in enumerate(dict_grammemes):
            if parent:
                children[parent].add(name)
            if gr.get(parent, None):  # parent's parent
                children[gr[parent]].add(name)

        # expand EXTRA_INCOMPATIBLE
        for grammeme, g_set in cls._EXTRA_INCOMPATIBLE.items():
            for g in g_set.copy():
                g_set.update(children[g])

        # fill GRAMMEME_INDICES and GRAMMEME_INCOMPATIBLE
        for index, (name, parent, alias, description) in enumerate(dict_grammemes):
            cls._GRAMMEME_INDICES[name] = index
            incompatible = cls._EXTRA_INCOMPATIBLE.get(name, set())
            incompatible = (incompatible | children[parent]) - set([name])

            cls._GRAMMEME_INCOMPATIBLE[name] = frozenset(incompatible)

    # XXX: do we still need these methods?
    @classmethod
    def _from_internal_tag(cls, tag):
        """ Return tag string given internal tag string """
        return tag

    @classmethod
    def _from_internal_grammeme(cls, grammeme):
        return grammeme

    def numeral_agreement_grammemes(self, num):
        if (num % 10 == 1) and (num % 100 != 11):
            index = 0
        elif (num % 10 >= 2) and (num % 10 <= 4) and (num % 100 < 10 or num % 100 >= 20):
            index = 1
        else:
            index = 2

        if self.POS not in ('NOUN', 'ADJF', 'PRTF'):
            return set([])

        if self.POS == 'NOUN' and self.case not in ('nomn', 'accs'):
            if index == 0:
                grammemes = set(['sing', self.case])
            else:
                grammemes = set(['plur', self.case])
        elif index == 0:
            if self.case == 'nomn':
                grammemes = self._NUMERAL_AGREEMENT_GRAMMEMES[0]
            else:
                grammemes = self._NUMERAL_AGREEMENT_GRAMMEMES[1]
        elif self.POS == 'NOUN' and index == 1:
            grammemes = self._NUMERAL_AGREEMENT_GRAMMEMES[2]
        elif self.POS in ('ADJF', 'PRTF') and self.gender == 'femn' and index == 1:
            grammemes = self._NUMERAL_AGREEMENT_GRAMMEMES[3]
        else:
            grammemes = self._NUMERAL_AGREEMENT_GRAMMEMES[4]
        return grammemes

    #@classmethod
    #def _clone_class(cls):
    #    Tag = type(cls.__name__, (cls,), {
    #         'KNOWN_GRAMMEMES': cls.KNOWN_GRAMMEMES.copy(),
    #    })
    #    # copyreg.pickle(Tag, pickle_tag)
    #    return Tag



class CyrillicOpencorporaTag(OpencorporaTag):
    """
    Tag class that uses Cyrillic tag names.

    .. warning::

        This class is experimental and incomplete, do not use
        it because it may be removed in future!
    """

    FORMAT = 'opencorpora-ext'

    _GRAMMEME_ALIAS_MAP = dict()

    @classmethod
    def _from_internal_tag(cls, tag):
        for name, alias in cls._GRAMMEME_ALIAS_MAP.items():
            if alias:
                tag = tag.replace(name, alias)
        return tag

    @classmethod
    def _from_internal_grammeme(cls, grammeme):
        return cls._GRAMMEME_ALIAS_MAP.get(grammeme, grammeme)

    @classmethod
    def _init_grammemes(cls, dict_grammemes):
        """
        Initialize various class attributes with grammeme
        information obtained from XML dictionary.
        """
        cls._init_alias_map(dict_grammemes)
        super(CyrillicOpencorporaTag, cls)._init_grammemes(dict_grammemes)

        GRAMMEME_INDICES = collections.defaultdict(int)
        for name, idx in cls._GRAMMEME_INDICES.items():
            GRAMMEME_INDICES[cls._from_internal_grammeme(name)] = idx
        cls._GRAMMEME_INDICES = GRAMMEME_INDICES

        GRAMMEME_INCOMPATIBLE = collections.defaultdict(set)
        for name, value in cls._GRAMMEME_INCOMPATIBLE.items():
            GRAMMEME_INCOMPATIBLE[cls._from_internal_grammeme(name)] = set([
                cls._from_internal_grammeme(gr) for gr in value
            ])
        cls._GRAMMEME_INCOMPATIBLE = GRAMMEME_INCOMPATIBLE

        cls._NON_PRODUCTIVE_GRAMMEMES = set([
            cls._from_internal_grammeme(gr) for gr in cls._NON_PRODUCTIVE_GRAMMEMES
        ])

    @classmethod
    def _init_alias_map(cls, dict_grammemes):
        for name, parent, alias, description in dict_grammemes:
            cls._GRAMMEME_ALIAS_MAP[name] = alias


def _translate_tag(tag, mapping):
    """
    Translate ``tag`` string according to ``mapping``, assuming grammemes
    are separated by commas or whitespaces. Commas/whitespaces positions
    are preserved.
    """
    if isinstance(tag, OpencorporaTag):
        tag = str(tag)
    return " ".join([
        _translate_comma_separated(whitespace_separated_part, mapping)
        for whitespace_separated_part in tag.split()
    ])


def _translate_comma_separated(tag_part, mapping):
    grammemes = [mapping.get(tok, tok) for tok in tag_part.split(',')]
    return ",".join(grammemes)


registry = dict()

for tag_type in [CyrillicOpencorporaTag, OpencorporaTag]:
    registry[tag_type.FORMAT] = tag_type
