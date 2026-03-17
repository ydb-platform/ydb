import unittest
import inspect

from betamax import exceptions


def exception_classes():
    for _, module_object in inspect.getmembers(exceptions):
        if inspect.isclass(module_object):
            yield module_object


class TestExceptions(unittest.TestCase):
    def test_all_exceptions_are_betamax_errors(self):
        for exception_class in exception_classes():
            assert isinstance(exception_class('msg'), exceptions.BetamaxError)

    def test_all_validation_errors_are_in_validation_error_map(self):
        validation_error_map_values = exceptions.validation_error_map.values()
        for exception_class in exception_classes():
            if exception_class.__name__ == 'ValidationError' or \
               not exception_class.__name__.endswith('ValidationError'):
                continue
            assert exception_class in validation_error_map_values

    def test_all_validation_errors_are_validation_errors(self):
        for exception_class in exception_classes():
            if not exception_class.__name__.endswith('ValidationError'):
                continue
            assert isinstance(exception_class('msg'),
                              exceptions.ValidationError)

    def test_invalid_option_is_validation_error(self):
        assert isinstance(exceptions.InvalidOption('msg'),
                          exceptions.ValidationError)

    def test_betamaxerror_repr(self):
        """Ensure errors don't raise exceptions in their __repr__.

        This should protect against regression. If this test starts failing,
        heavily modify it to not be flakey.
        """
        assert "BetamaxError" in repr(exceptions.BetamaxError('test'))
