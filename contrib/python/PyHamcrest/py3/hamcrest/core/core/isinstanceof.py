from typing import Type

from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.description import Description
from hamcrest.core.helpers.wrap_matcher import is_matchable_type
from hamcrest.core.matcher import Matcher

__author__ = "Jon Reid"
__copyright__ = "Copyright 2011 hamcrest.org"
__license__ = "BSD, see License.txt"


class IsInstanceOf(BaseMatcher[object]):
    def __init__(self, expected_type: Type) -> None:
        if not is_matchable_type(expected_type):
            raise TypeError("IsInstanceOf requires type or a tuple of classes and types")
        self.expected_type = expected_type

    def _matches(self, item: object) -> bool:
        return isinstance(item, self.expected_type)

    def describe_to(self, description: Description) -> None:
        try:
            type_description = self.expected_type.__name__
        except AttributeError:
            type_description = "one of %s" % ",".join(str(e) for e in self.expected_type)
        description.append_text("an instance of ").append_text(type_description)


def instance_of(atype: Type) -> Matcher[object]:
    """Matches if object is an instance of, or inherits from, a given type.

    :param atype: The type to compare against as the expected type or a tuple
        of types.

    This matcher checks whether the evaluated object is an instance of
    ``atype`` or an instance of any class that inherits from ``atype``.

    Example::

        instance_of(str)

    """
    return IsInstanceOf(atype)
