from mammoth import document_matchers
from ..testing import assert_equal


def test_equal_to_matcher_is_case_insensitive():
    matcher = document_matchers.equal_to("Heading 1")
    assert_equal(True, matcher.matches("heaDING 1"))
    assert_equal(False, matcher.matches("heaDING 2"))


def test_starts_with_matcher_matches_string_with_prefix():
    matcher = document_matchers.starts_with("Heading")
    assert_equal(True, matcher.matches("Heading"))
    assert_equal(True, matcher.matches("Heading 1"))
    assert_equal(False, matcher.matches("Custom Heading"))
    assert_equal(False, matcher.matches("Head"))
    assert_equal(False, matcher.matches("Header 2"))


def test_starts_with_matcher_is_case_insensitive():
    matcher = document_matchers.starts_with("Heading")
    assert_equal(True, matcher.matches("heaDING"))
