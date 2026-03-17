import traceback

from .base import Matcher
from .results import matched, unmatched


def raises(exception_matcher):
    return RaisesMatcher(exception_matcher)


class RaisesMatcher(Matcher):
    def __init__(self, exception_matcher):
        self._exception_matcher = exception_matcher

    def match(self, actual):
        if not callable(actual):
            return unmatched("was not callable")

        try:
            actual()
        except Exception as error:
            result = self._exception_matcher.match(error)
            if result.is_match:
                return matched()
            else:
                return unmatched("exception did not match: {0}\n\n{1}".format(
                    result.explanation,
                    traceback.format_exc(),
                ))

        return unmatched("did not raise exception")

    def describe(self):
        return "a callable raising: {0}".format(self._exception_matcher.describe())
