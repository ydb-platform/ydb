# -*- coding: utf-8 -*-

""" OneLogin_Saml2_XML class


Auxiliary class of SAML Python Toolkit.

"""

from os.path import join, dirname
from lxml import etree
import __res as arcadia_resfs
from onelogin.saml2 import compat
from onelogin.saml2.constants import OneLogin_Saml2_Constants
from onelogin.saml2.xmlparser import tostring, fromstring


for prefix, url in OneLogin_Saml2_Constants.NSMAP.items():
    etree.register_namespace(prefix, url)


class Resolver(etree.Resolver):
    def resolve(self, url, id, context):
        if url.endswith(".xsd"):
            schema_file = join(dirname(__file__), 'schemas', url)
            return self.resolve_string(arcadia_resfs.resfs_read(schema_file), context)


class OneLogin_Saml2_XML(object):
    _element_class = type(etree.Element('root'))
    _parse_etree = staticmethod(fromstring)
    _schema_class = etree.XMLSchema
    _text_class = compat.text_types
    _bytes_class = compat.bytes_type
    _unparse_etree = staticmethod(tostring)

    dump = staticmethod(etree.dump)
    make_root = staticmethod(etree.Element)
    make_child = staticmethod(etree.SubElement)

    @staticmethod
    def to_string(xml, **kwargs):
        """
        Serialize an element to an encoded string representation of its XML tree.
        :param xml: The root node
        :type xml: str|bytes|xml.dom.minidom.Document|etree.Element
        :returns: string representation of xml
        :rtype: string
        """

        if isinstance(xml, OneLogin_Saml2_XML._text_class):
            return xml

        if isinstance(xml, OneLogin_Saml2_XML._element_class):
            OneLogin_Saml2_XML.cleanup_namespaces(xml)
            return OneLogin_Saml2_XML._unparse_etree(xml, **kwargs)

        raise ValueError("unsupported type %r" % type(xml))

    @staticmethod
    def to_etree(xml):
        """
        Parses an XML document or fragment from a string.
        :param xml: the string to parse
        :type xml: str|bytes|xml.dom.minidom.Document|etree.Element
        :returns: the root node
        :rtype: OneLogin_Saml2_XML._element_class
        """
        if isinstance(xml, OneLogin_Saml2_XML._element_class):
            return xml
        if isinstance(xml, OneLogin_Saml2_XML._bytes_class):
            return OneLogin_Saml2_XML._parse_etree(xml, forbid_dtd=True, forbid_entities=True)
        if isinstance(xml, OneLogin_Saml2_XML._text_class):
            return OneLogin_Saml2_XML._parse_etree(compat.to_bytes(xml), forbid_dtd=True, forbid_entities=True)

        raise ValueError('unsupported type %r' % type(xml))

    @staticmethod
    def validate_xml(xml, schema, debug=False):
        """
        Validates a xml against a schema
        :param xml: The xml that will be validated
        :type xml: str|bytes|xml.dom.minidom.Document|etree.Element
        :param schema: The schema
        :type schema: string
        :param debug: If debug is active, the parse-errors will be showed
        :type debug: bool
        :returns: Error code or the DomDocument of the xml
        :rtype: xml.dom.minidom.Document
        """

        assert isinstance(schema, compat.str_type)
        try:
            xml = OneLogin_Saml2_XML.to_etree(xml)
        except Exception as e:
            if debug:
                print(e)
            return 'unloaded_xml'

        schema_file = join(dirname(__file__), 'schemas', schema)
        resolver_instance = Resolver()
        parser = etree.XMLParser(load_dtd=True)
        parser.resolvers.add(resolver_instance)
        schema_doc = etree.fromstring(arcadia_resfs.resfs_read(schema_file), parser=parser)
        xmlschema = OneLogin_Saml2_XML._schema_class(schema_doc)

        if not xmlschema.validate(xml):
            if debug:
                print('Errors validating the metadata: ')
                for error in xmlschema.error_log:
                    print(error.message)
            return 'invalid_xml'
        return xml

    @staticmethod
    def query(dom, query, context=None, tagid=None):
        """
        Extracts nodes that match the query from the Element

        :param dom: The root of the lxml objet
        :type: Element

        :param query: Xpath Expresion
        :type: string

        :param context: Context Node
        :type: DOMElement

        :param tagid: Tag ID
        :type query: String

        :returns: The queried nodes
        :rtype: list
        """
        if context is None:
            source = dom
        else:
            source = context

        if tagid is None:
            return source.xpath(query, namespaces=OneLogin_Saml2_Constants.NSMAP)
        else:
            return source.xpath(query, tagid=tagid, namespaces=OneLogin_Saml2_Constants.NSMAP)

    @staticmethod
    def cleanup_namespaces(tree_or_element, top_nsmap=None, keep_ns_prefixes=None):
        """
        Keeps the xmlns:xs namespace intact when etree.cleanup_namespaces is invoked.
        :param tree_or_element: An XML tree or element
        :type tree_or_element: etree.Element
        :param top_nsmap: A mapping from namespace prefixes to namespace URIs
        :type top_nsmap: dict
        :param keep_ns_prefixes: List of prefixes that should not be removed as part of the cleanup
        :type keep_ns_prefixes: list
        :returns: An XML tree or element
        :rtype: etree.Element
        """
        all_prefixes_to_keep = [
            OneLogin_Saml2_Constants.NS_PREFIX_XS,
            OneLogin_Saml2_Constants.NS_PREFIX_XSI,
            OneLogin_Saml2_Constants.NS_PREFIX_XSD
        ]

        if keep_ns_prefixes:
            all_prefixes_to_keep = list(set(all_prefixes_to_keep.extend(keep_ns_prefixes)))

        return etree.cleanup_namespaces(tree_or_element, keep_ns_prefixes=all_prefixes_to_keep)

    @staticmethod
    def extract_tag_text(xml, tagname):
        open_tag = compat.to_bytes("<%s" % tagname)
        close_tag = compat.to_bytes("</%s>" % tagname)

        xml = OneLogin_Saml2_XML.to_string(xml)
        start = xml.find(open_tag)
        assert start != -1

        end = xml.find(close_tag, start) + len(close_tag)
        assert end != -1
        return compat.to_string(xml[start:end])

    @staticmethod
    def element_text(node):
        # Double check, the LXML Parser already removes comments
        etree.strip_tags(node, etree.Comment)
        return node.text
