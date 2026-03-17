from functools import cache

from collada.xmlutil import ElementMaker, COLLADA_NS

E = ElementMaker(namespace=COLLADA_NS, nsmap={None: COLLADA_NS})


@cache
def tag(text, namespace=None):
    """
    Tag a text key with the collada namespace, by default:
    '{http://www.collada.org/2005/11/COLLADASchema}'

    :param string text:
      The text to be tagged, i.e. 'geometry'
    :param string namespace:
      The namespace to tag with (not including brackets)
      Will use default namespace if None is passed
    """
    if namespace is None:
        namespace = COLLADA_NS
    return '{%s}%s' % (namespace, text)


def tagger(namespace=None):
    """
    A closure, or function that returns a function.
    Returned function tags using a specified namespace.

    :param string namespace:
      The XML namespace to use to tag elements

    :return:
      tag() function
    """
    @cache
    def tag(text):
        return '{%s}%s' % (namespace, text)

    return tag


class DaeObject(object):
    """This class is the abstract interface to all collada objects.

    Every <tag> in a COLLADA that we recognize and load has mirror
    class deriving from this one. All instances will have at least
    a :meth:`load` method which creates the object from an xml node and
    an attribute called :attr:`xmlnode` with the ElementTree representation
    of the data. Even if it was created on the fly. If the object is
    not read-only, it will also have a :meth:`save` method which saves the
    object's information back to the :attr:`xmlnode` attribute.

    """

    xmlnode = None
    """ElementTree representation of the data."""

    @staticmethod
    def load(collada, localscope, node):
        """Load and return a class instance from an XML node.

        Inspect the data inside node, which must match
        this class tag and create an instance out of it.

        :param collada.Collada collada:
          The collada file object where this object lives
        :param dict localscope:
          If there is a local scope where we should look for local ids
          (sid) this is the dictionary. Otherwise empty dict ({})
        :param node:
          An Element from python's ElementTree API

        """
        raise Exception('Not implemented')

    def save(self):
        """Put all the data to the internal xml node (xmlnode) so it can be serialized."""


class DaeError(Exception):
    """General DAE exception."""

    def __init__(self, msg):
        super(DaeError, self).__init__()
        self.msg = msg

    def __str__(self):
        return type(self).__name__ + ': ' + self.msg

    def __repr__(self):
        return type(self).__name__ + '("' + self.msg + '")'


class DaeIncompleteError(DaeError):
    """Raised when needed data for an object isn't there."""


class DaeBrokenRefError(DaeError):
    """Raised when a referenced object is not found in the scope."""


class DaeMalformedError(DaeError):
    """Raised when data is found to be corrupted in some way."""


class DaeUnsupportedError(DaeError):
    """Raised when some unexpectedly unsupported feature is found."""


class DaeSaveValidationError(DaeError):
    """Raised when XML validation fails when saving."""
