from .base import Matcher
from .results import matched, unmatched, indented_list


def equal_to(value):
    return EqualToMatcher(value)


class EqualToMatcher(Matcher):
    def __init__(self, value):
        self._value = value

    def match(self, actual):
        if self._value == actual:
            return matched()
        else:
            return unmatched("was {0!r}".format(actual))

    def describe(self):
        return repr(self._value)


class AnyThingMatcher(Matcher):
    def match(self, actual):
        return matched()

    def describe(self):
        return "anything"


anything = AnyThingMatcher()


def all_of(*matchers):
    return AllOfMatcher(matchers)

class AllOfMatcher(Matcher):
    def __init__(self, matchers):
        self._matchers = matchers

    def match(self, actual):
        for matcher in self._matchers:
            result = matcher.match(actual)
            if not result.is_match:
                return result

        return matched()

    def describe(self):
        return "all of:{0}".format(indented_list(
            matcher.describe()
            for matcher in self._matchers
        ))


def any_of(*matchers):
    return AnyOfMatcher(matchers)

class AnyOfMatcher(Matcher):
    def __init__(self, matchers):
        self._matchers = matchers

    def match(self, actual):
        results = []
        for matcher in self._matchers:
            result = matcher.match(actual)
            if result.is_match:
                return result
            else:
                results.append(result)

        return unmatched("did not match any of:{0}".format(indented_list(
            "{0} [{1}]".format(matcher.describe(), result.explanation)
            for result, matcher in zip(results, self._matchers)
        )))

    def describe(self):
        return "any of:{0}".format(indented_list(
            matcher.describe()
            for matcher in self._matchers
        ))


def not_(matcher):
    return NotMatcher(matcher)

class NotMatcher(Matcher):
    def __init__(self, matcher):
        self._matcher = matcher

    def match(self, actual):
        result = self._matcher.match(actual)
        if result.is_match:
            return unmatched("matched: {0}".format(self._matcher.describe()))
        else:
            return matched()

    def describe(self):
        return "not: {0}".format(self._matcher.describe())
