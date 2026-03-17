
from collections import OrderedDict

from yargy.record import Record

from .attribute import (
    AttributeSchemeBase,
    RepeatableAttribute
)


class Fact(Record):
    __attributes__ = []
    _raw = None

    def __init__(self, **kwargs):
        for key in kwargs:
            if key not in self.__attributes__:
                raise KeyError(key)

        for key in self.__attributes__:
            if key in kwargs:
                value = kwargs[key]
            else:
                attribute = getattr(self, key)
                if isinstance(attribute, RepeatableAttribute):
                    value = []
                else:
                    value = attribute.default
            setattr(self, key, value)

    @property
    def as_json(self):
        return self._raw.as_json

    @property
    def spans(self):
        return sorted(
            self._raw.spans,
            key=lambda _: _.start
        )


def prepare_attribute(item):
    if isinstance(item, AttributeSchemeBase):
        return item
    else:
        from yargy.interpretation import attribute
        return attribute(item)


def fact(name, attributes):
    cls = type(
        str(name),
        (Fact,),
        dict(__attributes__=[],
             _raw=None)
    )

    for item in attributes:
        attribute = prepare_attribute(item)
        key = attribute.name
        cls.__attributes__.append(key)
        attribute = attribute.construct(cls)
        setattr(cls, str(key), attribute)

    return cls


class InterpretatorFact(Record):
    __attributes__ = ['attributes', 'repeatable', 'modified']

    def __init__(self, scheme):
        self.scheme = scheme
        self.repeatable = set()
        self.modified = set()
        self.attributes = {}
        for key in self.scheme.__attributes__:
            attribute = getattr(self.scheme, key)
            if isinstance(attribute, RepeatableAttribute):
                self.repeatable.add(key)
                value = []
            else:
                value = attribute.default
            self.attributes[key] = value

    def set(self, key, value):
        if key in self.repeatable:
            self.attributes[key].append(value)
        else:
            self.attributes[key] = value
        self.modified.add(key)

    def merge(self, fact):
        for key in fact.modified:
            value = fact.attributes[key]
            self.attributes[key] = value
            self.modified.add(key)

    @property
    def normalized(self):
        attributes = {}
        for key, value in self.attributes.items():
            if key in self.repeatable:
                value = [_.normalized for _ in value]
            elif key in self.modified:
                value = value.normalized
            attributes[key] = value
        fact = self.scheme(**attributes)
        fact._raw = self
        return fact

    @property
    def spans(self):
        for key, value in self.attributes.items():
            if key in self.repeatable:
                for item in value:
                    for span in item.spans:
                        yield span
            elif key in self.modified:
                for span in value.spans:
                    yield span

    @property
    def as_json(self):
        data = OrderedDict()
        for key in self.scheme.__attributes__:
            value = self.attributes[key]
            if key in self.repeatable:
                value = [_.as_json for _ in value]
            elif key in self.modified:
                value = value.as_json
            if value is not None:
                data[key] = value
        return data
