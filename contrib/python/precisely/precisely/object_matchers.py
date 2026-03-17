from .base import Matcher
from .results import matched, unmatched, indented_list
from .coercion import to_matcher


def has_attr(name, matcher):
    return HasAttr(name, to_matcher(matcher))

class HasAttr(Matcher):
    def __init__(self, name, matcher):
        self._name = name
        self._matcher = matcher
    
    def match(self, actual):
        if not hasattr(actual, self._name):
            return unmatched("was missing attribute {0}".format(self._name))
        else:
            actual_property = getattr(actual, self._name)
            property_result = self._matcher.match(actual_property)
            if property_result.is_match:
                return matched()
            else:
                return unmatched("attribute {0} {1}".format(self._name, property_result.explanation))
    
    def describe(self):
        return "object with attribute {0}: {1}".format(self._name, self._matcher.describe())


def has_attrs(*args, **kwargs):
    attrs = []
    if attrs is not None:
        for arg in args:
            if isinstance(arg, dict):
                attrs += arg.items()
            else:
                attrs.append(arg)
    
    attrs += kwargs.items()
    
    return HasAttrs(attrs)

class HasAttrs(Matcher):
    def __init__(self, matchers):
        self._matchers = [
            has_attr(name, matcher)
            for name, matcher in matchers
        ]
    
    def match(self, actual):
        for matcher in self._matchers:
            result = matcher.match(actual)
            if not result.is_match:
                return result
        return matched()
    
    def describe(self):
        return "object with attributes:{0}".format(indented_list(
            "{0}: {1}".format(matcher._name, matcher._matcher.describe())
            for matcher in self._matchers
        ))


def is_instance(type_):
    return IsInstance(type_)

class IsInstance(Matcher):
    def __init__(self, type_):
        self._type = type_
    
    def match(self, actual):
        if isinstance(actual, self._type):
            return matched()
        else:
            return unmatched("had type {0}".format(type(actual).__name__))
    
    def describe(self):
        return "is instance of {0}".format(self._type.__name__)
