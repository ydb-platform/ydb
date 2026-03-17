import functools

COLLADA_NS = 'http://www.collada.org/2005/11/COLLADASchema'
HAVE_LXML = False

try:
    from lxml import etree
    HAVE_LXML = True
except ImportError:
    from xml.etree import ElementTree as etree

ET = etree

try:
    from functools import partial
except ImportError:
    # fake it for pre-2.5 releases
    def partial(func, tag):
        return lambda *args, **kwargs: func(tag, *args, **kwargs)

if HAVE_LXML:
    from lxml.builder import E, ElementMaker

    def writeXML(xmlnode, fp):
        xmlnode.write(fp, pretty_print=True)
else:
    class ElementMaker(object):
        def __init__(self, namespace=None, nsmap=None):
            if namespace is not None:
                self._namespace = '{' + namespace + '}'
            else:
                self._namespace = None

        def __call__(self, tag, *children, **attrib):
            if self._namespace is not None and tag[0] != '{':
                tag = self._namespace + tag

            elem = etree.Element(tag, attrib)
            for item in children:
                if isinstance(item, dict):
                    elem.attrib.update(item)
                elif isinstance(item, str):
                    if len(elem):
                        elem[-1].tail = (elem[-1].tail or "") + item
                    else:
                        elem.text = (elem.text or "") + item
                elif etree.iselement(item):
                    elem.append(item)
                else:
                    raise TypeError("bad argument: %r" % item)
            return elem

        def __getattr__(self, tag):
            return functools.partial(self, tag)

    E = ElementMaker()

    if etree.VERSION[0:3] == '1.2':
        # in etree < 1.3, this is a workaround for suppressing prefixes

        def fixtag(tag, namespaces):
            import string
            # given a decorated tag (of the form {uri}tag), return prefixed
            # tag and namespace declaration, if any
            if isinstance(tag, etree.QName):
                tag = tag.text
            namespace_uri, tag = string.split(tag[1:], "}", 1)
            prefix = namespaces.get(namespace_uri)
            if namespace_uri not in namespaces:
                prefix = etree._namespace_map.get(namespace_uri)
                if namespace_uri not in etree._namespace_map:
                    prefix = "ns%d" % len(namespaces)
                namespaces[namespace_uri] = prefix
                if prefix == "xml":
                    xmlns = None
                else:
                    if prefix is not None:
                        nsprefix = ':' + prefix
                    else:
                        nsprefix = ''
                    xmlns = ("xmlns%s" % nsprefix, namespace_uri)
            else:
                xmlns = None
            if prefix is not None:
                prefix += ":"
            else:
                prefix = ''

            return "%s%s" % (prefix, tag), xmlns

        etree.fixtag = fixtag
        etree._namespace_map[COLLADA_NS] = None
    else:
        # For etree > 1.3, use register_namespace function
        etree.register_namespace('', COLLADA_NS)

    def indent(elem, level=0):
        i = "\n" + level * "  "
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = i + "  "
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
            for elem in elem:
                indent(elem, level + 1)
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = i

    def writeXML(xmlnode, fp):
        indent(xmlnode.getroot())
        xmlnode.write(fp)


def createElementTree(file):
    if not HAVE_LXML:
        return etree.ElementTree(element=None, file=file)
    from lxml.etree import XMLParser, parse
    parser = XMLParser(huge_tree=True)
    return parse(file, parser=parser)
