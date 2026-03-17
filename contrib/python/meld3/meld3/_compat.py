import re
import sys
PY3 = sys.version_info[0] == 3
from xml.etree.ElementTree import QName

try:
    import html.entities as htmlentitydefs
except ImportError: # Python 2.x
    import htmlentitydefs

try:
    from html.parser import HTMLParser
except ImportError: # python 2.x
    from HTMLParser import HTMLParser

try:
    # XXX reverse this, and maybe do BytesIO too
    from StringIO import StringIO
except ImportError: # Python 3.x
    from io import StringIO


try:
    bytes = bytes
except NameError:   # Python 2.5
    bytes = str

try:
    unichr = unichr
except NameError:   # Python 3.x
    unichr = chr
    def _b(x, encoding='latin1'):
        # x should be a str literal
        return bytes(x, encoding)
    def _u(x, encoding='latin1'):
        # x should be a str literal
        if isinstance(x, str):
            return x
        return str(x, encoding)

else:               # Python 2.x
    def _b(x, encoding='latin1'):
        # x should be a str literal
        return x
    def _u(x, encoding='latin1'):
        # x should be a str literal
        return unicode(x, encoding)

try:
    from types import StringTypes
except ImportError: # Python 3.x
    StringTypes = (str,)

#-----------------------------------------------------------------------------
# Begin fork from Python 2.6.8 stdlib:
#       - xml.elementtree.ElementTree._raise_serialization_error
#       - xml.elementtree.ElementTree._encode_entity
#       - xml.elementtree.ElementTree._namespace_map
#       - xml.elementtree.ElementTree.fixtag
#-----------------------------------------------------------------------------

_NON_ASCII_MIN = _u('\xc2\x80', 'utf-8')        # u'\u0080'
_NON_ASCII_MAX = _u('\xef\xbf\xbf', 'utf-8')    # u'\uffff'

_escape_map = {
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
}

_namespace_map = {
    # "well-known" namespace prefixes
    "http://www.w3.org/XML/1998/namespace": "xml",
    "http://www.w3.org/1999/xhtml": "html",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#": "rdf",
    "http://schemas.xmlsoap.org/wsdl/": "wsdl",
}

def _encode(s, encoding):
    try:
        return s.encode(encoding)
    except AttributeError:
        return s

def _raise_serialization_error(text):
    raise TypeError(
        "cannot serialize %r (type %s)" % (text, type(text).__name__)
        )

_pattern = None
def _encode_entity(text):
    # map reserved and non-ascii characters to numerical entities
    global _pattern
    if _pattern is None:
        _ptxt = r'[&<>\"' + _NON_ASCII_MIN + '-' + _NON_ASCII_MAX + ']+'
        #_pattern = re.compile(eval(r'u"[&<>\"\u0080-\uffff]+"'))
        _pattern = re.compile(_ptxt)

    def _escape_entities(m):
        out = []
        append = out.append
        for char in m.group():
            text = _escape_map.get(char)
            if text is None:
                text = "&#%d;" % ord(char)
            append(text)
        return ''.join(out)
    try:
        return _encode(_pattern.sub(_escape_entities, text), "ascii")
    except TypeError:
        _raise_serialization_error(text)

def fixtag(tag, namespaces):
    # given a decorated tag (of the form {uri}tag), return prefixed
    # tag and namespace declaration, if any
    if isinstance(tag, QName):
        tag = tag.text
    namespace_uri, tag = tag[1:].split("}", 1)
    prefix = namespaces.get(namespace_uri)
    if prefix is None:
        prefix = _namespace_map.get(namespace_uri)
        if prefix is None:
            prefix = "ns%d" % len(namespaces)
        namespaces[namespace_uri] = prefix
        if prefix == "xml":
            xmlns = None
        else:
            xmlns = ("xmlns:%s" % prefix, namespace_uri)
    else:
        xmlns = None
    return "%s:%s" % (prefix, tag), xmlns
#-----------------------------------------------------------------------------
# End fork from Python 2.6.8 stdlib
#-----------------------------------------------------------------------------
