# This file is part of Androguard.
#
# Copyright (C) 2012, Anthony Desnos <desnos at t0t0.fr>
# All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
import sys
import binascii
import logging

from pyaxmlparser.axmlparser import AXMLParser
from pyaxmlparser.utils import format_value
import pyaxmlparser.constants as const
from lxml import etree

log = logging.getLogger("pyaxmlparser.axmlprinter")


class AXMLPrinter:
    """
    Converter for AXML Files into a lxml ElementTree, which can easily be
    converted into XML.

    A Reference Implementation can be found at http://androidxref.com/9.0.0_r3/
    xref/frameworks/base/tools/aapt/XMLNode.cpp
    """
    __charrange = None
    __replacement = None

    def __init__(self, raw_buff):
        self.axml = AXMLParser(raw_buff)

        self.root = None
        self.packerwarning = False
        cur = []

        while self.axml.is_valid():
            _type = next(self.axml)

            if _type == const.START_TAG:
                name = self._fix_name(self.axml.name)
                uri = self._print_namespace(self.axml.namespace)
                tag = "{}{}".format(uri, name)

                comment = self.axml.comment
                if comment:
                    if self.root is None:
                        log.warning("Can not attach comment with content '{}' without root!".format(comment))
                    else:
                        cur[-1].append(etree.Comment(comment))

                log.debug("START_TAG: {} (line={})".format(tag, self.axml.m_lineNumber))
                elem = etree.Element(tag, nsmap=self.axml.nsmap)

                for i in range(self.axml.getAttributeCount()):
                    uri = self._print_namespace(self.axml.getAttributeNamespace(i))
                    name = self._fix_name(self.axml.getAttributeName(i))
                    value = self._fix_value(self._get_attribute_value(i))

                    log.debug("found an attribute: {}{}='{}'".format(uri, name, value.encode("utf-8")))
                    if "{}{}".format(uri, name) in elem.attrib:
                        log.warning("Duplicate attribute '{}{}'! Will overwrite!".format(uri, name))
                    elem.set("{}{}".format(uri, name), value)

                if self.root is None:
                    self.root = elem
                else:
                    if not cur:
                        # looks like we lost the root?
                        log.error("No more elements available to attach to! Is the XML malformed?")
                        break
                    cur[-1].append(elem)
                cur.append(elem)

            if _type == const.END_TAG:
                if not cur:
                    log.warning("Too many END_TAG! No more elements available to attach to!")

                name = self.axml.name
                uri = self._print_namespace(self.axml.namespace)
                tag = "{}{}".format(uri, name)
                if cur[-1].tag != tag:
                    log.warning(
                        "Closing tag '{}' does not match current stack! "
                        "At line number: {}. Is the XML malformed?".format(
                            self.axml.name, self.axml.m_lineNumber
                        )
                    )
                cur.pop()
            if _type == const.TEXT:
                log.debug("TEXT for {}".format(cur[-1]))
                cur[-1].text = self.axml.text
            if _type == const.END_DOCUMENT:
                # Check if all namespace mappings are closed
                if len(self.axml.namespaces) > 0:
                    log.warning(
                        "Not all namespace mappings were closed! Malformed AXML?")
                break

    def get_buff(self):
        """
        Returns the raw XML file without prettification applied.

        :returns: bytes, encoded as UTF-8
        """
        return self.get_xml(pretty=False)

    def get_xml(self, pretty=True):
        """
        Get the XML as an UTF-8 string

        :returns: bytes encoded as UTF-8
        """
        return etree.tostring(self.root, encoding="utf-8", pretty_print=pretty)

    def get_xml_obj(self):
        """
        Get the XML as an ElementTree object

        :returns: :class:`lxml.etree.Element`
        """
        return self.root

    def is_valid(self):
        """
        Return the state of the AXMLParser.
        If this flag is set to False, the parsing has failed, thus
        the resulting XML will not work or will even be empty.
        """
        return self.axml.is_valid()

    def is_packed(self):
        """
        Returns True if the AXML is likely to be packed

        Packers do some weird stuff and we try to detect it.
        Sometimes the files are not packed but simply broken or compiled with
        some broken version of a tool.
        Some file corruption might also be appear to be a packed file.

        :returns: True if packer detected, False otherwise
        """
        return self.packerwarning

    def _get_attribute_value(self, index):
        """
        Wrapper function for format_value
        to resolve the actual value of an attribute in a tag
        :param index: index of the current attribute
        :return: formatted value
        """
        _type = self.axml.getAttributeValueType(index)
        _data = self.axml.getAttributeValueData(index)

        return format_value(_type, _data, lambda _: self.axml.getAttributeValue(index))

    def _fix_name(self, name):
        """
        Apply some fixes to element named and attribute names.
        Try to get conform to:
        > Like element names, attribute names are case-sensitive and must start with a letter or underscore.
        > The rest of the name can contain letters, digits, hyphens, underscores, and periods.
        See: https://msdn.microsoft.com/en-us/library/ms256152(v=vs.110).aspx

        :param name: Name of the attribute
        :return: a fixed version of the name
        """
        if not name[0].isalpha() and name[0] != "_":
            log.warning("Invalid start for name '{}'".format(name))
            self.packerwarning = True
            name = "_{}".format(name)
        if name.startswith("android:"):
            # Seems be a common thing...
            # Actually this means that the Manifest is likely to be broken, as
            # usually no namespace URI is set in this case.
            log.warning(
                "Name '{}' starts with 'android:' prefix! "
                "The Manifest seems to be broken? Removing prefix.".format(
                    name
                )
            )
            self.packerwarning = True
            name = name[len("android:"):]
        if ":" in name:
            # Print out an extra warning
            log.warning("Name seems to contain a namespace prefix: '{}'".format(name))
        if not re.match(r"^[a-zA-Z0-9._-]*$", name):
            log.warning("Name '{}' contains invalid characters!".format(name))
            self.packerwarning = True
            name = re.sub(r"[^a-zA-Z0-9._-]", "_", name)

        return name

    def _fix_value(self, value):
        """
        Return a cleaned version of a value
        according to the specification:
        > Char	   ::=   	#x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]

        See https://www.w3.org/TR/xml/#charsets

        :param value: a value to clean
        :return: the cleaned value
        """
        if not self.__charrange or not self.__replacement:
            if sys.maxunicode == 0xFFFF:
                # Fix for python 2.x, surrogate pairs does not match in regex
                self.__charrange = re.compile(
                    u'^([\u0020-\uD7FF\u0009\u000A\u000D\uE000-\uFFFD]|[\uD800-\uDBFF][\uDC00-\uDFFF])*$')
                # TODO: this regex is slightly wrong... surrogates are not matched as pairs.
                self.__replacement = re.compile(u'[^\u0020-\uDBFF\u0009\u000A\u000D\uE000-\uFFFD\uDC00-\uDFFF]')
            else:
                self.__charrange = re.compile(u'^[\u0020-\uD7FF\u0009\u000A\u000D\uE000-\uFFFD\U00010000-\U0010FFFF]*$')
                self.__replacement = re.compile(u'[^\u0020-\uD7FF\u0009\u000A\u000D\uE000-\uFFFD\U00010000-\U0010FFFF]')

        # Reading string until \x00. This is the same as aapt does.
        if "\x00" in value:
            self.packerwarning = True
            log.warning(
                "Null byte found in attribute value at position {}: "
                "Value(hex): '{}'".format(
                    value.find("\x00"),
                    binascii.hexlify(value.encode("utf-8"))
                )
            )
            value = value[:value.find("\x00")]

        if not self.__charrange.match(value):
            log.warning("Invalid character in value found. Replacing with '_'.")
            self.packerwarning = True
            value = self.__replacement.sub('_', value)
        return value

    def _print_namespace(self, uri):
        if uri != "":
            uri = "{{{}}}".format(uri)
        return uri
