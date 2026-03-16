
from yargy.record import Record
from yargy.check import assert_equals

from .normalizer import (
    InflectedNormalizer,
    NormalizedNormalizer,
    ConstNormalizer,
    FunctionNormalizer,
    FunctionFunctionNormalizer,
    MorphFunctionNormalizer,
)


class AttributeSchemeBase(Record):
    __attributes__ = ['name']


class AttributeScheme(AttributeSchemeBase):
    __attributes__ = ['name', 'default']

    def __init__(self, name, default=None):
        self.name = name
        self.default = default

    def repeatable(self):
        return RepeatableAttributeScheme(self)

    def construct(self, fact):
        return Attribute(fact, self.name, self.default)


class RepeatableAttributeScheme(AttributeSchemeBase):
    def __init__(self, attribute):
        assert_equals(attribute.default, None)
        self.name = attribute.name

    def construct(self, fact):
        return RepeatableAttribute(fact, self.name)


class AttributeBase(Record):
    __attributes__ = ['fact', 'name']

    def __init__(self, fact, name):
        self.fact = fact
        self.name = name

    @property
    def label(self):
        return '{fact}.{name}'.format(
            fact=self.fact.__name__,
            name=self.name
        )


class Attribute(AttributeBase):
    __attributes__ = ['fact', 'name', 'default']

    def __init__(self, fact, name, default):
        self.fact = fact
        self.name = name
        self.default = default

    def inflected(self, grams={'nomn', 'sing'}):
        return InflectedAttribute(self, grams)

    def normalized(self):
        return NormalizedAttribute(self)

    def const(self, value):
        return ConstAttribute(self, value)

    def custom(self, function):
        return FunctionAttribute(self, function)


class RepeatableAttribute(AttributeBase):
    pass


class MorphAttribute(AttributeBase):
    pass


class InflectedAttribute(MorphAttribute):
    __attributes__ = ['attribute', 'grams']

    def __init__(self, attribute, grams):
        self.attribute = attribute
        self.grams = grams

    def custom(self, function):
        return InflectedFunctionAttribute(self.attribute, self.grams, function)

    @property
    def normalizer(self):
        return InflectedNormalizer(self.grams)


class NormalizedAttribute(MorphAttribute):
    __attributes__ = ['attribute']

    def __init__(self, attribute):
        self.attribute = attribute

    def custom(self, function):
        return NormalizedFunctionAttribute(self.attribute, function)

    @property
    def normalizer(self):
        return NormalizedNormalizer()


class CustomAttribute(AttributeBase):
    pass


class ConstAttribute(CustomAttribute):
    __attributes__ = ['attribute', 'value']

    def __init__(self, attribute, value):
        self.attribute = attribute
        self.value = value

    @property
    def normalizer(self):
        return ConstNormalizer(self.value)


class FunctionAttribute(CustomAttribute):
    __attributes__ = ['attribute', 'function']

    def __init__(self, attribute, function):
        self.attribute = attribute
        self.function = function

    def custom(self, function):
        return FunctionFunctionAttribute(
            self.attribute,
            self.function,
            function
        )

    @property
    def normalizer(self):
        return FunctionNormalizer(self.function)


class FunctionFunctionAttribute(CustomAttribute):
    __attributes__ = ['attribute', 'first', 'second']

    def __init__(self, attribute, first, second):
        self.attribute = attribute
        self.first = first
        self.second = second

    @property
    def normalizer(self):
        return FunctionFunctionNormalizer(self.first, self.second)


class InflectedFunctionAttribute(MorphAttribute, CustomAttribute):
    __attributes__ = ['attribute', 'grams', 'function']

    def __init__(self, attribute, grams, function):
        self.attribute = attribute
        self.grams = grams
        self.function = function

    @property
    def normalizer(self):
        return MorphFunctionNormalizer(
            InflectedNormalizer(self.grams),
            self.function
        )


class NormalizedFunctionAttribute(MorphAttribute, CustomAttribute):
    __attributes__ = ['attribute', 'function']

    def __init__(self, attribute, function):
        self.attribute = attribute
        self.function = function

    @property
    def normalizer(self):
        return MorphFunctionNormalizer(
            NormalizedNormalizer(),
            self.function
        )
