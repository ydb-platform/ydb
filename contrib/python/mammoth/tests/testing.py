import os

from precisely import assert_that, equal_to


def generate_test_path(path):
    this_dir = os.path.dirname(__file__)
    return os.path.join(this_dir, "test-data", path)


def assert_equal(expected, actual):
    assert_that(actual, equal_to(expected))


def assert_raises(exception, func):
    try:
        func()
        assert False, "Expected " + exception.__name__
    except exception as error:
        return error

