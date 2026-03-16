from flex.exceptions import (
    ErrorList,
    ErrorDict,
)


def test_bare_instantiation():
    error_dict = ErrorDict()
    assert 'a' not in error_dict
    assert isinstance(error_dict['a'], ErrorList)


def test_instantiation_with_initial_value():
    value = {'a': 3}
    error_dict = ErrorDict(value)
    assert 'a' in error_dict
    assert 3 in error_dict['a']
