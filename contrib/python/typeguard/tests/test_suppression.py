import pytest

from typeguard import TypeCheckError, check_type, suppress_type_checks, typechecked


def test_contextmanager_typechecked():
    @typechecked
    def foo(x: str) -> None:
        pass

    with suppress_type_checks():
        foo(1)


def test_contextmanager_check_type():
    with suppress_type_checks():
        check_type(1, str)


def test_contextmanager_nesting():
    with suppress_type_checks(), suppress_type_checks():
        check_type(1, str)

    pytest.raises(TypeCheckError, check_type, 1, str)


def test_contextmanager_exception():
    """
    Test that type check suppression stops even if an exception is raised within the
    context manager block.

    """
    with pytest.raises(RuntimeError):
        with suppress_type_checks():
            raise RuntimeError

    pytest.raises(TypeCheckError, check_type, 1, str)


@suppress_type_checks
def test_decorator_typechecked():
    @typechecked
    def foo(x: str) -> None:
        pass

    foo(1)


@suppress_type_checks
def test_decorator_check_type():
    check_type(1, str)


def test_decorator_exception():
    """
    Test that type check suppression stops even if an exception is raised from a
    decorated function.

    """

    @suppress_type_checks
    def foo():
        raise RuntimeError

    with pytest.raises(RuntimeError):
        foo()

    pytest.raises(TypeCheckError, check_type, 1, str)
