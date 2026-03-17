"""
Tests for depricated methods.
"""
from six import PY2


if PY2:
    def test_has_key():
        """
        The now-depricated has_keys method
        """
        from attrdict.dictionary import AttrDict

        mapping = AttrDict(
            {'foo': 'bar', frozenset((1, 2, 3)): 'abc', 1: 2}
        )
        empty = AttrDict()

        assert mapping.has_key('foo')
        assert not empty.has_key('foo')

        assert mapping.has_key(frozenset((1, 2, 3)))
        assert not empty.has_key(frozenset((1, 2, 3)))

        assert mapping.has_key(1)
        assert not empty.has_key(1)

        assert not mapping.has_key('banana')
        assert not empty.has_key('banana')
