from .base import Matcher, is_matcher
from .comparison_matchers import contains_string, greater_than, greater_than_or_equal_to, less_than, less_than_or_equal_to, starts_with, close_to
from .core_matchers import equal_to, anything, all_of, any_of, not_
from .object_matchers import has_attr, has_attrs, is_instance
from .iterable_matchers import all_elements, contains_exactly, includes, is_sequence
from .feature_matchers import has_feature
from .function_matchers import raises
from .mapping_matchers import is_mapping, mapping_includes
from .results import indent as _indent


__all__ = [
    "assert_that",
    "Matcher",
    "is_matcher",
    "contains_string",
    "greater_than",
    "greater_than_or_equal_to",
    "less_than",
    "less_than_or_equal_to",
    "close_to",
    "starts_with",
    "equal_to",
    "anything",
    "all_of",
    "any_of",
    "not_",
    "has_attr",
    "has_attrs",
    "is_instance",
    "all_elements",
    "contains_exactly",
    "includes",
    "is_same_sequence",
    "is_sequence",
    "has_feature",
    "is_mapping",
    "mapping_includes",
    "raises",
]

# Deprecated
is_same_sequence = is_sequence
instance_of = is_instance


def assert_that(value, matcher):
    result = matcher.match(value)
    if not result.is_match:
        raise AssertionError("\nExpected:{0}\nbut:{1}".format(
            _indent("\n" + matcher.describe()),
            _indent("\n" + result.explanation),
        ))
