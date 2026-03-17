"""
Test the merge function
"""


def test_merge():
    """
    merge function.
    """
    from attrdict.merge import merge

    left = {
        'baz': 'qux',
        'mismatch': False,
        'sub': {'alpha': 'beta', 1: 2},
    }
    right = {
        'lorem': 'ipsum',
        'mismatch': True,
        'sub': {'alpha': 'bravo', 3: 4},
    }

    assert merge({}, {}) == {}
    assert merge(left, {}) == left
    assert merge({}, right) == right
    assert merge(left, right) == \
           {
               'baz': 'qux',
               'lorem': 'ipsum',
               'mismatch': True,
               'sub': {'alpha': 'bravo', 1: 2, 3: 4}
           }
