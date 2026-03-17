
from inspect import isclass

from yargy.record import Record
from yargy.check import assert_subclass
from yargy.token import (
    is_token,
    join_tokens,
    get_tokens_span
)
from .attribute import (
    AttributeBase,
    MorphAttribute,
    CustomAttribute
)
from .normalizer import (
    Normalizer,
    ConstNormalizer
)
from .fact import (
    Fact,
    InterpretatorFact
)


class InterpretatorInput(Record):
    __attributes__ = ['items', 'key']

    def __init__(self, items, key=None):
        self.items = list(items)
        self.key = key


class InterpretatorResult(Record):
    normalized = None
    spans = None


class Chain(InterpretatorResult):
    __attributes__ = ['tokens', 'key']

    def __init__(self, tokens, key):
        self.tokens = tokens
        self.key = key

    @property
    def normalized(self):
        return join_tokens(self.tokens)

    @property
    def spans(self):
        yield get_tokens_span(self.tokens)

    @property
    def as_json(self):
        return self.normalized


class FactResult(InterpretatorResult):
    __attributes__ = ['fact']

    def __init__(self, fact):
        self.fact = fact

    @property
    def normalized(self):
        return self.fact.normalized

    @property
    def spans(self):
        for span in self.fact.spans:
            yield span

    @property
    def as_json(self):
        return self.fact.as_json


class AttributeResult(InterpretatorResult):
    __attributes__ = ['value', 'attribute']

    def __init__(self, value, attribute):
        self.value = value
        self.attribute = attribute

    @property
    def normalized(self):
        return self.value.normalized

    @property
    def spans(self):
        for span in self.value.spans:
            yield span

    @property
    def as_json(self):
        return self.value.as_json


class NormalizerResult(InterpretatorResult):
    __attributes__ = ['value', 'input']

    def __init__(self, value, input):
        self.value = value
        self.input = input

    @property
    def normalized(self):
        return self.value

    @property
    def spans(self):
        for span in self.input.spans:
            yield span

    @property
    def as_json(self):
        if isinstance(self.value, InterpretatorResult):
            return self.value.as_json
        else:
            return self.value


class Interpretator(Record):
    label = 'Interpretator'

    def __call__(self, input):
        raise NotImplementedError


class FactInterpretator(Interpretator):
    __attributes__ = ['fact']

    def __init__(self, fact):
        assert_subclass(fact, Fact)
        self.fact = fact

    def __call__(self, input):
        fact = InterpretatorFact(self.fact)
        for item in input.items:
            if isinstance(item, AttributeResult) and issubclass(self.fact, item.attribute.fact):
                fact.set(
                    item.attribute.name,
                    item.value
                )
            elif isinstance(item, FactResult) and issubclass(item.fact.scheme, self.fact):
                fact.merge(item.fact)
        return FactResult(fact)

    @property
    def label(self):
        return self.fact.__name__


class AttributeInterpretator(Interpretator):
    __attributes__ = ['attribute']

    def __init__(self, attribute):
        self.attribute = attribute

    def __call__(self, input):
        items = input.items
        if all(is_token(_) for _ in items):
            value = Chain(items, input.key)
        elif len(items) == 1:
            item = items[0]
            if isinstance(item, AttributeResult):
                value = item.value
            elif isinstance(item, (NormalizerResult, FactResult)):
                value = item
            else:
                raise TypeError(type(item))
        else:
            raise TypeError('{attribute!r} -> {types!r}'.format(
                attribute=self,
                types=[type(_) for _ in items]
            ))
        return AttributeResult(value, self.attribute)

    @property
    def label(self):
        return self.attribute.label


class NormalizerInterpretator(Interpretator):
    __attributes__ = ['normalizer']

    def __init__(self, normalizer):
        self.normalizer = normalizer

    def __call__(self, input):
        if isinstance(self.normalizer, ConstNormalizer):
            value = self.normalizer.value
        else:
            items = input.items
            if all(is_token(_) for _ in items):
                input = Chain(items, input.key)
            elif len(items) == 1:
                input = items[0]
            else:
                raise TypeError('{normalizer!r} -> {types!r}'.format(
                    normalizer=self,
                    types=[type(_) for _ in items]
                ))
            value = self.normalizer(input)
        return NormalizerResult(value, input)

    @property
    def label(self):
        return self.normalizer.label


class AttributeNormalizerInterpretator(AttributeInterpretator, NormalizerInterpretator):
    __attributes__ = ['attribute', 'normalizer']

    def __init__(self, attribute, normalizer):
        AttributeInterpretator.__init__(self, attribute)
        NormalizerInterpretator.__init__(self, normalizer)

    def __call__(self, items):
        value = NormalizerInterpretator.__call__(self, items)
        return AttributeResult(value, self.attribute)

    @property
    def label(self):
        return '{attribute}.{normalizer}'.format(
            attribute=self.attribute.label,
            normalizer=self.normalizer.label
        )


def prepare_attribute_interpretator(item):
    if isinstance(item, MorphAttribute) or isinstance(item, CustomAttribute):
        return AttributeNormalizerInterpretator(
            item.attribute,
            item.normalizer
        )
    else:
        return AttributeInterpretator(item)


def prepare_token_interpretator(item):
    if isinstance(item, AttributeBase):
        return prepare_attribute_interpretator(item)
    elif isinstance(item, Normalizer):
        return NormalizerInterpretator(item)
    else:
        raise TypeError(item)


def prepare_rule_interpretator(item):
    if isinstance(item, Interpretator):
        return item
    elif isclass(item) and issubclass(item, Fact):
        return FactInterpretator(item)
    elif isinstance(item, AttributeBase):
        return prepare_attribute_interpretator(item)
    elif isinstance(item, Normalizer):
        return NormalizerInterpretator(item)
    else:
        raise TypeError(item)
