from .base import Matcher
from .results import matched, unmatched
from .coercion import to_matcher


def has_feature(name, extract, matcher):
    return HasFeatureMatcher(name, extract, to_matcher(matcher))

class HasFeatureMatcher(Matcher):
    def __init__(self, name, extract, matcher):
        self._name = name
        self._extract = extract
        self._matcher = matcher
    
    def match(self, actual):
        actual_feature = self._extract(actual)
        feature_result = self._matcher.match(actual_feature)
        if feature_result.is_match:
            return matched()
        else:
            return unmatched(self._description(feature_result.explanation))
    
    def describe(self):
        return self._description(self._matcher.describe())
    
    def _description(self, value):
        return "{0}: {1}".format(self._name, value)
