from flex.exceptions import (
    ErrorList,
)


def test_bare_instantiation():
    error_list = ErrorList()
    assert error_list == []


def test_instantiation_with_list_initial_value():
    error_list = ErrorList([1, 2, 3])
    assert error_list == [1, 2, 3]


def test_instantiation_with_non_list_initial_value():
    error_list = ErrorList('abc')
    assert error_list == ['abc']


def test_adding_list_of_errors_extends_list():
    error_list = ErrorList()
    error_list.add_error([1, 2])
    error_list.add_error([3, 4])
    assert error_list == [1, 2, 3, 4]


def test_adding_string_error_appends():
    error_list = ErrorList()
    error_list.add_error([1, 2])
    error_list.add_error('abc')
    assert error_list == [1, 2, 'abc']


def test_adding_mixed_list():
    error_list = ErrorList()
    error_list.add_error([1, 2, [3, 4], 'abc'])
    assert error_list == [1, 2, 3, 4, 'abc']
