# encoding:UTF-8
####################################################################
#                                                                  #
# THIS FILE IS PART OF THE pycollada LIBRARY SOURCE CODE.          #
# USE, DISTRIBUTION AND REPRODUCTION OF THIS LIBRARY SOURCE IS     #
# GOVERNED BY A BSD-STYLE SOURCE LICENSE INCLUDED WITH THIS SOURCE #
# IN 'COPYING'. PLEASE READ THESE TERMS BEFORE DISTRIBUTING.       #
#                                                                  #
# THE pycollada SOURCE CODE IS (C) COPYRIGHT 2011                  #
# by Jeff Terrace and contributors                                 #
#                                                                  #
####################################################################

"""This module contains helper classes and functions for working
with the COLLADA 1.4.1 schema."""

import io
import lxml
import lxml.etree

from .resources import resource_string

# get a copy of the XML schema
COLLADA_SCHEMA_1_4_1 = resource_string("schema-1.4.1.xml")
XML_XSD = resource_string("xsd.xml")


class ColladaResolver(lxml.etree.Resolver):
    """COLLADA XML Resolver. If a known URL referenced
    from the COLLADA spec is resolved, a cached local
    copy is returned instead of initiating a network
    request"""

    def resolve(self, url, id, context):
        """Currently Resolves:
         * http://www.w3.org/2001/03/xml.xsd
        """
        if url == 'http://www.w3.org/2001/03/xml.xsd':
            return self.resolve_string(XML_XSD, context)
        else:
            return None


class ColladaValidator(object):
    """Validates a collada lxml document"""

    def __init__(self):
        """Initializes the validator"""
        self.COLLADA_SCHEMA_1_4_1_DOC = None
        self._COLLADA_SCHEMA_1_4_1_INSTANCE = None

    def _getColladaSchemaInstance(self):
        if self._COLLADA_SCHEMA_1_4_1_INSTANCE is None:
            self._parser = lxml.etree.XMLParser()
            self._parser.resolvers.add(ColladaResolver())
            self.COLLADA_SCHEMA_1_4_1_DOC = lxml.etree.parse(
                io.BytesIO(bytes(COLLADA_SCHEMA_1_4_1, encoding='utf-8')),
                self._parser)
            self._COLLADA_SCHEMA_1_4_1_INSTANCE = lxml.etree.XMLSchema(
                self.COLLADA_SCHEMA_1_4_1_DOC)
        return self._COLLADA_SCHEMA_1_4_1_INSTANCE

    COLLADA_SCHEMA_1_4_1_INSTANCE = property(_getColladaSchemaInstance)
    """An instance of lxml.XMLSchema that can be used to validate"""

    def validate(self, *args, **kwargs):
        """A wrapper for lxml.XMLSchema.validate"""
        return self.COLLADA_SCHEMA_1_4_1_INSTANCE.validate(*args, **kwargs)
