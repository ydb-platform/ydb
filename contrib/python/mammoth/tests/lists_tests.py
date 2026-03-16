from mammoth.lists import unique
from .testing import assert_equal


def test_unique_of_empty_list_is_empty_list():
    assert_equal([], unique([]))


def test_unique_removes_duplicates_while_preserving_order():
    assert_equal(["apple", "banana"], unique(["apple", "banana", "apple"]))
