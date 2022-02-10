#!/usr/bin/env python
# -*- coding: utf-8 -*-
from hamcrest import assert_that


def exception_to_string(e):
    return "=" * 80 + str(e) + "=" * 80


class CompositeAssertionError(AssertionError):
    def __init__(self, list_of_exceptions):
        super(CompositeAssertionError, self).__init__()
        self.__list_of_exceptions = list_of_exceptions

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "with assertions =\n{}".format("\n".join(map(exception_to_string, self.__list_of_exceptions)))


class CompositeAssert(object):
    def __init__(self):
        super(CompositeAssert, self).__init__()
        self.__list_of_assertion_errors = []

    def __enter__(self):
        return self

    def assert_that(self, arg1, arg2=None, arg3=''):
        try:
            if arg3:
                arg3 = '\n' + arg3
            assert_that(arg1, arg2, arg3)
        except AssertionError as e:
            self.__list_of_assertion_errors.append(e)

    def finish(self):
        return self.__exit__(None, None, None)

    def __exit__(self, type_, value, traceback):
        if self.__list_of_assertion_errors:
            raise CompositeAssertionError(self.__list_of_assertion_errors)
        return False


# ToDo: Refactor to use CompositeAssert instead of this.
class CompositeCheckResult(object):
    def __init__(self):
        self.__result = True
        self.__reason = []

    @property
    def result(self):
        return self.__result

    @property
    def description(self):
        return '\n'.join(self.__reason)

    def check(self, condition, description=""):
        if not condition:
            self.set_failed(description)

    def set_failed(self, description=""):
        self.__result = False
        if description:
            self.__reason.append(description)

    def __nonzero__(self):
        return self.__result
