
from functools import wraps

from yargy.tokenizer import INT
from yargy.token import (
    is_tag_token,
    is_morph_token
)

from .constructors import (
    Predicate,
    PredicateScheme,
    ParameterPredicate,
    ParameterPredicateScheme
)


__all__ = [
    'eq',
    'caseless',
    'in_',
    'in_caseless',
    'gte',
    'lte',
    'length_eq',
    'normalized',
    'dictionary',
    'gram',
    'type',
    'tag',
    'custom',
    'true',
    'is_lower',
    'is_upper',
    'is_title',
    'is_capitalized',
    'is_single'
]


def int_required(method):
    @wraps(method)
    def wrapper(self, token):
        if token.type == INT:
            value = int(token.value)
            return method(self, value)
        else:
            return False
    return wrapper


def morph_required(method):
    @wraps(method)
    def wrapper(self, token):
        if not is_morph_token(token):
            return False
        else:
            return method(self, token)
    return wrapper


def mock_tokenizer():
    from yargy.tokenizer import MorphTokenizer
    return MorphTokenizer()


def mock_context():
    from yargy.parser import Context
    return Context(mock_tokenizer())


def tokenize(string):
    tokenizer = mock_tokenizer()
    return list(tokenizer(string))


def activate(scheme):
    return scheme.activate(mock_context())


class true(Predicate):
    """Всегда возвращает True

    >>> predicate = true()
    >>> predicate(False)
    True

    """

    def __call__(self, token):
        return True


class is_lower(Predicate):
    """str.islower

    >>> predicate = is_lower()
    >>> a, b = tokenize('xxx Xxx')
    >>> predicate(a)
    True
    >>> predicate(b)
    False

    """

    def __call__(self, token):
        return token.value.islower()


class is_upper(Predicate):
    """str.isupper

    >>> predicate = is_upper()
    >>> a, b = tokenize('XXX xxx')
    >>> predicate(a)
    True
    >>> predicate(b)
    False

    """

    def __call__(self, token):
        return token.value.isupper()


class is_title(Predicate):
    """str.istitle

    >>> predicate = is_title()
    >>> a, b = tokenize('XXX Xxx')
    >>> predicate(a)
    False
    >>> predicate(b)
    True

    """

    def __call__(self, token):
        return token.value.istitle()


class is_capitalized(Predicate):
    """Слово написано с большой буквы

    >>> predicate = is_capitalized()
    >>> a, b, c = tokenize('Xxx XXX xxX')
    >>> predicate(a)
    True
    >>> predicate(b)
    True
    >>> predicate(c)
    False

    """

    def __call__(self, token):
        return token.value[0].isupper()


class eq(ParameterPredicate):
    """a == b

    >>> predicate = eq('1')
    >>> token, = tokenize('1')
    >>> predicate(token)
    True

    """

    def __call__(self, token):
        return token.value == self.value

    @property
    def label(self):
        return "'%s'" % self.value


class length_eq(ParameterPredicate):
    """len(a) == b

    >>> predicate = length_eq(3)
    >>> a, b = tokenize('XXX 123')
    >>> predicate(a)
    True
    >>> predicate(b)
    True

    """

    def __call__(self, token):
        return len(token.value) == self.value


class gte(ParameterPredicate):
    """a >= b

    >>> predicate = gte(4)
    >>> a, b, c = tokenize('3 5 C')
    >>> predicate(a)
    False
    >>> predicate(b)
    True
    >>> predicate(c)
    False

    """

    @int_required
    def __call__(self, value):
        return value >= self.value


class lte(ParameterPredicate):
    """a <= b

    >>> predicate = lte(4)
    >>> a, b, c = tokenize('3 5 C')
    >>> predicate(a)
    True
    >>> predicate(b)
    False
    >>> predicate(c)
    False

    """

    @int_required
    def __call__(self, value):
        return value <= self.value


class caseless(ParameterPredicate):
    """a.lower() == b.lower()

    >>> predicate = caseless('Рано')
    >>> token, = tokenize('РАНО')
    >>> predicate(token)
    True

    """

    def __init__(self, value):
        super(caseless, self).__init__(value.lower())

    def __call__(self, token):
        return token.value.lower() == self.value


class in_(ParameterPredicate):
    """a in b

    >>> predicate = in_({'S', 'M', 'L'})
    >>> a, b = tokenize('S 1')
    >>> predicate(a)
    True
    >>> predicate(b)
    False

    """

    def __call__(self, token):
        return token.value in self.value

    @property
    def label(self):
        return 'in_(...)'


class in_caseless(ParameterPredicate):
    """a.lower() in b

    >>> predicate = in_caseless({'S', 'M', 'L'})
    >>> a, b = tokenize('S m')
    >>> predicate(a)
    True
    >>> predicate(b)
    True

    """

    def __init__(self, value):
        value = {_.lower() for _ in value}
        super(in_caseless, self).__init__(value)

    def __call__(self, token):
        return token.value.lower() in self.value

    @property
    def label(self):
        return 'in_caseless(...)'


class normalized(ParameterPredicateScheme):
    """Нормальная форма слова == value

    >>> a = activate(normalized('сталь'))
    >>> b = activate(normalized('стать'))
    >>> token, = tokenize('стали')
    >>> a(token)
    True
    >>> b(token)
    True

    """

    def activate(self, context):
        normalized = context.tokenizer.morph.normalized(self.value)
        return DictionaryPredicate(normalized)


class dictionary(ParameterPredicateScheme):
    """Нормальная форма слова in value

    >>> predicate = activate(dictionary({'учитель', 'врач'}))
    >>> a, b = tokenize('учителя врачи')
    >>> predicate(a)
    True
    >>> predicate(b)
    True

    """

    def activate(self, context):
        normalized = set()
        for item in self.value:
            normalized.update(context.tokenizer.morph.normalized(item))
        return DictionaryPredicate(normalized)

    @property
    def label(self):
        return 'dictionary(...)'


class DictionaryPredicate(ParameterPredicate):
    def __call__(self, token):
        if is_morph_token(token):
            return any(
                _.normalized in self.value
                for _ in token.forms
            )
        else:
            value = token.normalized
            return value in self.value

    @property
    def label(self):
        return 'dictionary(...)'


class gram(ParameterPredicateScheme):
    """value есть среди граммем слова

    >>> a = activate(gram('NOUN'))
    >>> b = activate(gram('VERB'))
    >>> token, = tokenize('стали')
    >>> a(token)
    True
    >>> b(token)
    True

    """

    def activate(self, context):
        context.tokenizer.morph.check_gram(self.value)
        return GramPredicate(self.value)


class GramPredicate(ParameterPredicate):
    @morph_required
    def __call__(self, token):
        return any(
            self.value in _.grams
            for _ in token.forms
        )

    def constrain(self, token):
        return token.constrained([
            _ for _ in token.forms
            if self.value in _.grams
        ])

    @property
    def label(self):
        return "gram('%s')" % self.value


class type(ParameterPredicateScheme):
    """Тип токена равен value

    >>> predicate = activate(type('INT'))
    >>> a, b = tokenize('3 раза')
    >>> predicate(a)
    True
    >>> predicate(b)
    False

    """

    def activate(self, context):
        context.tokenizer.check_type(self.value)
        return TypePredicate(self.value)


class TypePredicate(ParameterPredicate):
    def __call__(self, token):
        return token.type == self.value

    @property
    def label(self):
        return 'type({value!r})'.format(value=self.value)


class tag(ParameterPredicateScheme):
    """Тег токена равен value

    """

    def activate(self, context):
        context.tagger.check_tag(self.value)
        return TagPredicate(self.value)


class TagPredicate(ParameterPredicate):
    def __call__(self, token):
        if is_tag_token(token):
            return token.tag == self.value
        return False

    @property
    def label(self):
        return 'tag({value!r})'.format(value=self.value)


class is_single(Predicate):
    """Слово в единственном числе

    >>> predicate = is_single()
    >>> token, = tokenize('слово')
    >>> predicate(token)
    True

    """

    def is_single(self, form):
        number = form.grams.number
        return number.single or number.only_single

    @morph_required
    def __call__(self, token):
        return any(
            self.is_single(_)
            for _ in token.forms
        )

    def constrain(self, token):
        return token.constrained([
            _ for _ in token.forms
            if self.is_single(_)
        ])


class custom(PredicateScheme):
    """function в качестве предиката

    >>> from math import log
    >>> f = lambda x: int(log(int(x), 10)) == 2
    >>> predicate = activate(custom(f, types=INT))
    >>> a, b = tokenize('12 123')
    >>> predicate(a)
    False
    >>> predicate(b)
    True

    """

    __attributes__ = ['function', 'types']

    def __init__(self, function, types=None):
        self.function = function
        if types is not None and not isinstance(types, (tuple, list)):
            types = [types]
        self.types = types

    def activate(self, context):
        if self.types:
            for type in self.types:
                context.tokenizer.check_type(type)
        return CustomPredicate(self.function, self.types)


class CustomPredicate(Predicate):
    __attributes__ = ['function', 'types']

    def __init__(self, function, types):
        self.function = function
        self.types = types

    def __call__(self, token):
        if self.types and token.type not in self.types:
            return False
        return self.function(token.value)

    @property
    def label(self):
        return 'custom({name})'.format(
            name=self.function.__name__
        )
