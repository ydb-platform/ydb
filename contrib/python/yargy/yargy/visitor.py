
from .check import assert_subclass


class Visitor(object):
    def resolve_method(self, item):
        for cls in item.__class__.__mro__:
            name = 'visit_' + cls.__name__
            method = getattr(self, name, None)
            if method:
                return method
        raise ValueError('no method for {type!r}'.format(
            type=type(item)
        ))

    def visit(self, item):
        return self.resolve_method(item)(item)


class TransformatorsComposition(Visitor):
    def __init__(self, transformators):
        for transformator in transformators:
            assert_subclass(transformator, Visitor)
        self.transformators = transformators

    def __call__(self, item):
        for transformator in self.transformators:
            item = transformator()(item)
        return item
