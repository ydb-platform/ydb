from typing import Hashable, Mapping, MutableMapping, TypeVar, Union

from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.description import Description
from hamcrest.core.helpers.hasmethod import hasmethod
from hamcrest.core.helpers.wrap_matcher import wrap_matcher
from hamcrest.core.matcher import Matcher

__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"


K = TypeVar("K", bound=Hashable)  # TODO - covariant?
V = TypeVar("V")


class IsDictContaining(BaseMatcher[Mapping[K, V]]):
    def __init__(self, key_matcher: Matcher[K], value_matcher: Matcher[V]) -> None:
        self.key_matcher = key_matcher
        self.value_matcher = value_matcher

    def _matches(self, item: Mapping[K, V]) -> bool:
        if hasmethod(item, "items"):
            for key, value in item.items():
                if self.key_matcher.matches(key) and self.value_matcher.matches(value):
                    return True
        return False

    def describe_to(self, description: Description) -> None:
        description.append_text("a dictionary containing [").append_description_of(
            self.key_matcher
        ).append_text(": ").append_description_of(self.value_matcher).append_text("]")

    def describe_mismatch(self, item: Mapping[K, V], mismatch_description: Description) -> None:
        key_matches = self._matching_keys(item)
        if len(key_matches) == 1:
            key, value = key_matches.popitem()
            mismatch_description.append_text("value for ").append_description_of(key).append_text(
                " "
            )
            self.value_matcher.describe_mismatch(value, mismatch_description)
        else:
            super().describe_mismatch(item, mismatch_description)

    def describe_match(self, item: Mapping[K, V], match_description: Description) -> None:
        key_matches = self._matching_keys(item)
        if len(key_matches) == 1:
            key, value = key_matches.popitem()
            match_description.append_text("value for ").append_description_of(key).append_text(" ")
            self.value_matcher.describe_mismatch(value, match_description)
        else:
            super().describe_match(item, match_description)

    def _matching_keys(self, item) -> MutableMapping[K, V]:
        key_matches: MutableMapping[K, V] = {}
        if hasmethod(item, "items"):
            for key, value in item.items():
                if self.key_matcher.matches(key):
                    key_matches[key] = value
        return key_matches


def has_entry(
    key_match: Union[K, Matcher[K]], value_match: Union[V, Matcher[V]]
) -> Matcher[Mapping[K, V]]:
    """Matches if dictionary contains key-value entry satisfying a given pair
    of matchers.

    :param key_match: The matcher to satisfy for the key, or an expected value
        for :py:func:`~hamcrest.core.core.isequal.equal_to` matching.
    :param value_match: The matcher to satisfy for the value, or an expected
        value for :py:func:`~hamcrest.core.core.isequal.equal_to` matching.

    This matcher iterates the evaluated dictionary, searching for any key-value
    entry that satisfies ``key_match`` and ``value_match``. If a matching entry
    is found, ``has_entry`` is satisfied.

    Any argument that is not a matcher is implicitly wrapped in an
    :py:func:`~hamcrest.core.core.isequal.equal_to` matcher to check for
    equality.

    Examples::

        has_entry(equal_to('foo'), equal_to(1))
        has_entry('foo', 1)

    """
    return IsDictContaining(wrap_matcher(key_match), wrap_matcher(value_match))
