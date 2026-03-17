from tests.utils import (
    check_if_error_message_equal,
    find_message_in_errors,
    find_matching_paths,
)


def test_foo():
    formatted_msg = u'Unknown type: unknown-type'
    unformatted_msg = u'Unknown type: {0}'

    assert check_if_error_message_equal(formatted_msg, unformatted_msg)


#
# find_message_in_errors
#
def test_matched_string_returns_namespace_path():
    """
    If a string is passed in as errors, the namespace should be returned if the
    string is a match.
    """
    errors = 'foo'
    path = find_message_in_errors('foo', errors, namespace='namespace')
    assert set(path) == set(['namespace'])


def test_non_matching_string_returns_empty():
    """
    If a string is passed in as errors, the namespace should be returned if the
    string is a match.
    """
    errors = 'foo'
    path = find_message_in_errors('not-a-match', errors)
    assert set(path) == set([])


def test_list_matches_return_element_index():
    errors = ['bar', 'foo']
    path = find_message_in_errors('foo', errors)
    assert set(path) == set(['1'])


def test_dictionary_matches_return_element_key():
    errors = {
        'bar': 'foo',
    }
    path = find_message_in_errors('foo', errors)
    assert set(path) == set(['bar'])


def test_nested_matches_return_full_namespace():
    errors = {
        'bar': ['foo'],
    }
    path = find_message_in_errors('foo', errors)
    assert set(path) == set(['bar.0'])


def test_a_complex_nested_match():
    errors = {
        'bar': [{'baz': 'foo'}],
    }
    path = find_message_in_errors('foo', errors)
    assert set(path) == set(['bar.0.baz'])


def test_multiple_matches_return_all_namespaces():
    errors = {
        'bar': [{'baz': 'foo'}],
        'bop': 'foo',
    }
    path = find_message_in_errors('foo', errors)
    assert set(path) == set(['bop', 'bar.0.baz'])


#
# find_message_paths tests
#
def test_simple_path_match():
    paths = ['foo']
    assert find_matching_paths('foo', paths)


def test_exact_path_match():
    paths = ['foo.bar']
    assert find_matching_paths('foo.bar', paths)


def test_missing_at_beginning_match():
    paths = ['0.foo.bar']
    assert find_matching_paths('foo.bar', paths)


def test_missing_at_middle_match():
    paths = ['foo.0.bar']
    assert find_matching_paths('foo.bar', paths)


def test_missing_at_end_match():
    paths = ['foo.bar.0']
    assert find_matching_paths('foo.bar', paths)


def test_simple_mismatch():
    paths = ['bar.foo']
    assert not find_matching_paths('foo.bar', paths)


def test_other_mismatch():
    paths = ['foo.0.bar']
    assert not find_matching_paths('foo.0.0.bar', paths)


def test_special_start_of_path_character_with_no_matches():
    paths = ['start.required']
    assert not find_matching_paths('^required', paths)

def test_special_start_of_path_character_with_matches():
    paths = ['required.extra']
    assert find_matching_paths('^required', paths)


def test_embedded_special_chars_should_match():
    paths = ['$ref.extra']
    assert find_matching_paths('$ref', paths)


def test_special_end_of_path_character_with_match():
    paths = ['start.extra']
    assert find_matching_paths('extra$', paths)


def test_special_end_of_path_character_should_not_match():
    paths = ['extra.end']
    assert not find_matching_paths('extra$', paths)
