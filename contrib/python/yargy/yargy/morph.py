
from functools import lru_cache

from pymorphy2 import MorphAnalyzer as PymorphyAnalyzer

from .record import Record


def pymorphy2_311_hotfix():
    # https://github.com/pymorphy2/pymorphy2/issues/160#issuecomment-1486657176

    from inspect import getfullargspec
    from pymorphy2.units.base import BaseAnalyzerUnit

    def _get_param_names_311(klass):
        if klass.__init__ is object.__init__:
            return []
        args = getfullargspec(klass.__init__).args
        return sorted(args[1:])

    setattr(BaseAnalyzerUnit, '_get_param_names', _get_param_names_311)


pymorphy2_311_hotfix()


class Gender(Record):
    __attributes__ = ['male', 'female', 'neutral', 'bi', 'general']

    def __init__(self, grams):
        self.male = 'masc' in grams
        self.female = 'femn' in grams
        self.neutral = 'neut' in grams
        # https://github.com/OpenCorpora/opencorpora/issues/795
        self.bi = 'Ms-f' in grams or 'ms-f' in grams
        self.general = 'GNdr' in grams


class Number(Record):
    __attributes__ = ['single', 'plural', 'only_single', 'only_plural']

    def __init__(self, grams):
        self.single = 'sing' in grams
        self.plural = 'plur' in grams
        self.only_single = 'Sgtm' in grams
        self.only_plural = 'Pltm' in grams


class Case(Record):
    __attributes__ = ['mask', 'fixed']

    def __init__(self, grams):
        self.mask = [
            (_ in grams)
            for _ in ['nomn', 'gent', 'datv', 'accs', 'ablt', 'loct', 'voct']
        ]
        self.fixed = 'Fixd' in grams


class Grams(Record):
    __attributes__ = ['values']

    def __init__(self, values):
        self.values = values

    @property
    def gender(self):
        return Gender(self)

    @property
    def number(self):
        return Number(self)

    @property
    def case(self):
        return Case(self)

    def __contains__(self, value):
        return value in self.values

    def __repr__(self):
        values = sorted(self.values)
        return 'Grams({values})'.format(
            values=','.join(values)
        )

    def _repr_pretty_(self, printer, cycle):
        printer.text(repr(self))


class Form(Record):
    __attributes__ = ['normalized', 'grams']

    def __init__(self, normalized, grams, raw=None):
        self.normalized = normalized
        self.grams = grams
        self.raw = raw

    def inflect(self, grams={'nomn', 'sing'}):
        record = self.raw.inflect(grams)
        if not record:
            return self.normalized
        return record.word

    def __repr__(self):
        return 'Form({self.normalized!r}, {self.grams!r})'.format(self=self)

    def _repr_pretty_(self, printer, cycle):
        printer.text(repr(self))


def prepare_form(raw):
    normalized = raw.normal_form
    grams = Grams(raw.tag.grammemes)
    return Form(normalized, grams, raw=raw)


class MorphAnalyzer(object):
    def __init__(self, raw=None):
        if not raw:
            raw = PymorphyAnalyzer()
        self.raw = raw

    def check_gram(self, gram):
        if not self.raw.TagClass.grammeme_is_known(gram):
            raise ValueError(gram)

    def __call__(self, word):
        records = self.raw.parse(word)
        return [prepare_form(_) for _ in records]

    def normalized(self, word):
        return {_.normalized for _ in self(word)}


CACHE_SIZE = 10000


class CachedMorphAnalyzer(MorphAnalyzer):
    def __init__(self):
        super(CachedMorphAnalyzer, self).__init__()

    __call__ = lru_cache(CACHE_SIZE)(MorphAnalyzer.__call__)
