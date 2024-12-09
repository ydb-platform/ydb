# Copyright (C) Jean-Paul Calderone
# Copyright (C) Twisted Matrix Laboratories.
# See LICENSE for details.
"""
Helpers for the OpenSSL test suite, largely copied from
U{Twisted<http://twistedmatrix.com/>}.
"""

from six import PY2


# This is the UTF-8 encoding of the SNOWMAN unicode code point.
NON_ASCII = b"\xe2\x98\x83".decode("utf-8")


def is_consistent_type(theType, name, *constructionArgs):
    """
    Perform various assertions about *theType* to ensure that it is a
    well-defined type.  This is useful for extension types, where it's
    pretty easy to do something wacky.  If something about the type is
    unusual, an exception will be raised.

    :param theType: The type object about which to make assertions.
    :param name: A string giving the name of the type.
    :param constructionArgs: Positional arguments to use with
        *theType* to create an instance of it.
    """
    assert theType.__name__ == name
    assert isinstance(theType, type)
    instance = theType(*constructionArgs)
    assert type(instance) is theType
    return True


class EqualityTestsMixin(object):
    """
    A mixin defining tests for the standard implementation of C{==} and C{!=}.
    """

    def anInstance(self):
        """
        Return an instance of the class under test.  Each call to this method
        must return a different object.  All objects returned must be equal to
        each other.
        """
        raise NotImplementedError()

    def anotherInstance(self):
        """
        Return an instance of the class under test.  Each call to this method
        must return a different object.  The objects must not be equal to the
        objects returned by C{anInstance}.  They may or may not be equal to
        each other (they will not be compared against each other).
        """
        raise NotImplementedError()

    def test_identicalEq(self):
        """
        An object compares equal to itself using the C{==} operator.
        """
        o = self.anInstance()
        assert o == o

    def test_identicalNe(self):
        """
        An object doesn't compare not equal to itself using the C{!=} operator.
        """
        o = self.anInstance()
        assert not (o != o)

    def test_sameEq(self):
        """
        Two objects that are equal to each other compare equal to each other
        using the C{==} operator.
        """
        a = self.anInstance()
        b = self.anInstance()
        assert a == b

    def test_sameNe(self):
        """
        Two objects that are equal to each other do not compare not equal to
        each other using the C{!=} operator.
        """
        a = self.anInstance()
        b = self.anInstance()
        assert not (a != b)

    def test_differentEq(self):
        """
        Two objects that are not equal to each other do not compare equal to
        each other using the C{==} operator.
        """
        a = self.anInstance()
        b = self.anotherInstance()
        assert not (a == b)

    def test_differentNe(self):
        """
        Two objects that are not equal to each other compare not equal to each
        other using the C{!=} operator.
        """
        a = self.anInstance()
        b = self.anotherInstance()
        assert a != b

    def test_anotherTypeEq(self):
        """
        The object does not compare equal to an object of an unrelated type
        (which does not implement the comparison) using the C{==} operator.
        """
        a = self.anInstance()
        b = object()
        assert not (a == b)

    def test_anotherTypeNe(self):
        """
        The object compares not equal to an object of an unrelated type (which
        does not implement the comparison) using the C{!=} operator.
        """
        a = self.anInstance()
        b = object()
        assert a != b

    def test_delegatedEq(self):
        """
        The result of comparison using C{==} is delegated to the right-hand
        operand if it is of an unrelated type.
        """

        class Delegate(object):
            def __eq__(self, other):
                # Do something crazy and obvious.
                return [self]

        a = self.anInstance()
        b = Delegate()
        assert (a == b) == [b]

    def test_delegateNe(self):
        """
        The result of comparison using C{!=} is delegated to the right-hand
        operand if it is of an unrelated type.
        """

        class Delegate(object):
            def __ne__(self, other):
                # Do something crazy and obvious.
                return [self]

        a = self.anInstance()
        b = Delegate()
        assert (a != b) == [b]


# The type name expected in warnings about using the wrong string type.
if PY2:
    WARNING_TYPE_EXPECTED = "unicode"
else:
    WARNING_TYPE_EXPECTED = "str"
