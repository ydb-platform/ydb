
from yargy.record import Record
from yargy.check import assert_type
from yargy.token import (
    join_normalized_tokens,
    join_inflected_tokens
)


class Normalizer(Record):
    def __call__(self, _):
        raise NotImplementedError


class MorphNormalizer(Normalizer):
    pass


class NormalizedNormalizer(MorphNormalizer):
    label = 'normalized()'

    def custom(self, function):
        return MorphFunctionNormalizer(self, function)

    def __call__(self, item):
        from .interpretator import Chain

        assert_type(item, Chain)
        if item.key:
            return item.key
        else:
            return join_normalized_tokens(item.tokens)


class InflectedNormalizer(MorphNormalizer):
    __attributes__ = ['grams']

    def __init__(self, grams=None):
        self.grams = grams

    def custom(self, function):
        return MorphFunctionNormalizer(self, function)

    def __call__(self, item):
        from .interpretator import Chain

        assert_type(item, Chain)
        return join_inflected_tokens(item.tokens, self.grams)

    @property
    def label(self):
        return 'inflected({grams})'.format(
            grams=', '.join(self.grams)
        )


class CustomNormalizer(Normalizer):
    pass


class ConstNormalizer(CustomNormalizer):
    __attributes__ = ['value']

    def __init__(self, value):
        self.value = value

    def __call__(self, _):
        return self.value

    @property
    def label(self):
        return 'const({value!r})'.format(
            value=self.value
        )


class FunctionNormalizer(CustomNormalizer):
    __attributes__ = ['function']

    def __init__(self, function):
        self.function = function

    def custom(self, function):
        return FunctionFunctionNormalizer(
            self.function,
            function
        )

    def __call__(self, item):
        value = item.normalized
        return self.function(value)

    @property
    def label(self):
        return 'custom({name})'.format(
            name=self.function.__name__
        )


class FunctionFunctionNormalizer(CustomNormalizer):
    __attributes__ = ['first', 'second']

    def __init__(self, first, second):
        self.first = first
        self.second = second

    def __call__(self, item):
        value = item.normalized
        return self.second(self.first(value))

    @property
    def label(self):
        return 'custom({first}).custom({second})'.format(
            first=self.first.__name__,
            second=self.second.__name__
        )


class MorphFunctionNormalizer(MorphNormalizer, CustomNormalizer):
    __attributes__ = ['morph', 'function']

    def __init__(self, morph, function):
        self.morph = morph
        self.function = function

    def __call__(self, items):
        value = self.morph(items)
        return self.function(value)

    @property
    def label(self):
        return '{morph}.{function}'.format(
            morph=self.morph.label,
            function=self.function.__name__
        )
