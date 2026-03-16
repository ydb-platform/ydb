try:
    from itertools import zip_longest
except ImportError:
    from itertools import izip_longest as zip_longest

from .base import Matcher
from .results import matched, unmatched, indented_list, indexed_indented_list, Result
from .coercion import to_matcher


def contains_exactly(*matchers):
    return ContainsExactlyMatcher([to_matcher(matcher) for matcher in matchers])


class ContainsExactlyMatcher(Matcher):
    def __init__(self, matchers):
        self._matchers = matchers

    def match(self, actual):
        values = _to_list_or_mismatch(actual)

        if isinstance(values, Result):
            return values
        elif len(values) == 0 and len(self._matchers) != 0:
            return unmatched("iterable was empty")
        else:
            matches = _Matches(values)
            for matcher in self._matchers:
                result = matches.match(matcher)
                if not result.is_match:
                    return result
            return matches.match_remaining()

    def describe(self):
        elements_description = indented_list(
            matcher.describe()
            for matcher in self._matchers
        )

        if len(self._matchers) == 0:
            return _empty_iterable_description
        elif len(self._matchers) == 1:
            return "iterable containing 1 element:{0}".format(
                elements_description,
            )
        else:
            return "iterable containing these {0} elements in any order:{1}".format(
                len(self._matchers),
                elements_description,
            )


def includes(*matchers):
    return IncludesMatcher([to_matcher(matcher) for matcher in matchers])


class IncludesMatcher(Matcher):
    def __init__(self, matchers):
        self._matchers = matchers

    def match(self, actual):
        values = _to_list_or_mismatch(actual)

        if isinstance(values, Result):
            return values
        elif len(values) == 0 and len(self._matchers) != 0:
            return unmatched("iterable was empty")

        matches = _Matches(values)
        for matcher in self._matchers:
            result = matches.match(matcher)
            if not result.is_match:
                return result
        return matched()

    def describe(self):
        return "iterable including elements:{0}".format(indented_list(
            matcher.describe()
            for matcher in self._matchers
        ))


class _Matches(object):
    def __init__(self, values):
        self._values = values
        self._is_matched = [False] * len(values)

    def match(self, matcher):
        mismatches = []
        for index, (is_matched, value) in enumerate(zip(self._is_matched, self._values)):
            if is_matched:
                result = unmatched("already matched")
            else:
                result = matcher.match(value)

            if result.is_match:
                self._is_matched[index] = True
                return result
            else:
                mismatches.append(result)

        return unmatched("was missing element:{0}\nThese elements were in the iterable, but did not match the missing element:{1}".format(
            indented_list([matcher.describe()]),
            indented_list("{0}: {1}".format(repr(value), mismatch.explanation) for value, mismatch in zip(self._values, mismatches)),
        ))

    def match_remaining(self):
        if all(self._is_matched):
            return matched()
        else:
            return unmatched("had extra elements:{0}".format(indented_list(
                repr(value)
                for is_matched, value in zip(self._is_matched, self._values)
                if not is_matched
            )))


def is_sequence(*matchers):
    return IsSequenceMatcher([to_matcher(matcher) for matcher in matchers])


class IsSequenceMatcher(Matcher):
    _missing = object()

    def __init__(self, matchers):
        self._matchers = matchers

    def match(self, actual):
        values = _to_list_or_mismatch(actual)

        if isinstance(values, Result):
            return values

        elif len(values) == 0 and len(self._matchers) != 0:
            return unmatched("iterable was empty")

        extra = []
        for index, (matcher, value) in enumerate(zip_longest(self._matchers, values, fillvalue=self._missing)):
            if matcher is self._missing:
                extra.append(value)
            elif value is self._missing:
                return unmatched("element at index {0} was missing".format(index))
            else:
                result = matcher.match(value)
                if not result.is_match:
                    return unmatched("element at index {0} mismatched:{1}".format(index, indented_list([result.explanation])))

        if extra:
            return unmatched("had extra elements:{0}".format(indented_list(map(repr, extra))))
        else:
            return matched()

    def describe(self):
        if len(self._matchers) == 0:
            return _empty_iterable_description
        else:
            return "iterable containing in order:{0}".format(indexed_indented_list(
                matcher.describe()
                for matcher in self._matchers
            ))


def all_elements(matcher):
    return AllElementsMatcher(matcher)


class AllElementsMatcher(Matcher):

    def __init__(self, matcher):
        self._element_matcher = matcher

    def match(self, actual):
        values = _to_list_or_mismatch(actual)

        if isinstance(values, Result):
            return values

        for index, value in enumerate(values):
            result = self._element_matcher.match(value)
            if not result.is_match:
                return unmatched("element at index {0} mismatched: {1}".format(index, result.explanation))

        return matched()

    def describe(self):
        return "all elements of iterable match: {0}".format(self._element_matcher.describe())


_empty_iterable_description = "empty iterable"


def _to_list_or_mismatch(iterable):
    try:
        iterator = iter(iterable)
    except TypeError:
        return unmatched("was not iterable\nwas {0}".format(repr(iterable)))

    return list(iterator)
