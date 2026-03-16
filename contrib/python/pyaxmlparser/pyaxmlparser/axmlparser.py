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

import logging
from struct import unpack

import pyaxmlparser.constants as const
from pyaxmlparser import bytecode
from pyaxmlparser.stringblock import StringBlock
from pyaxmlparser.resources import public
from .arscutil import ARSCHeader

log = logging.getLogger("pyaxmlparser.axmlparser")


class AXMLParser(object):
    """
    AXMLParser reads through all chunks in the AXML file
    and implements a state machine to return information about
    the current chunk, which can then be read by :class:`~AXMLPrinter`.

    An AXML file is a file which contains multiple chunks of data, defined
    by the `ResChunk_header`.
    There is no real file magic but as the size of the first header is fixed
    and the `type` of the `ResChunk_header` is set to `RES_XML_TYPE`, a file
    will usually start with `0x03000800`.
    But there are several examples where the `type` is set to something
    else, probably in order to fool parsers.

    Typically the AXMLParser is used in a loop which terminates if `m_event` is set to `END_DOCUMENT`.
    You can use the `next()` function to get the next chunk.
    Note that not all chunk types are yielded from the iterator! Some chunks are processed in
    the AXMLParser only.
    The parser will set `is_valid()` to False if it parses something not valid.
    Messages what is wrong are logged.

    See http://androidxref.com/9.0.0_r3/xref/frameworks/base/libs/androidfw/include/androidfw/ResourceTypes.h#563
    """
    def __init__(self, raw_buff):
        self._reset()

        self._valid = True
        self.axml_tampered = False
        self.buff = bytecode.BuffHandle(raw_buff)

        # Minimum is a single ARSCHeader, which would be a strange edge case...
        if self.buff.size() < 8:
            log.error("Filesize is too small to be a valid AXML file! Filesize: {}".format(self.buff.size()))
            self._valid = False
            return

        # This would be even stranger, if an AXML file is larger than 4GB...
        # But this is not possible as the maximum chunk size is a unsigned 4 byte int.
        if self.buff.size() > 0xFFFFFFFF:
            log.error("Filesize is too large to be a valid AXML file! Filesize: {}".format(self.buff.size()))
            self._valid = False
            return

        try:
            axml_header = ARSCHeader(self.buff)
        except AssertionError as e:
            log.error("Error parsing first resource header: %s", e)
            self._valid = False
            return

        self.filesize = axml_header.size

        if axml_header.header_size == 28024:
            # Can be a common error: the file is not an AXML but a plain XML
            # The file will then usually start with '<?xm' / '3C 3F 78 6D'
            log.warning("Header size is 28024! Are you trying to parse a plain XML file?")

        if axml_header.header_size != 8:
            log.error(
                "This does not look like an AXML file. "
                "header size does not equal 8! header size = {}".format(
                    axml_header.header_size
                )
            )
            self._valid = False
            return

        if self.filesize > self.buff.size():
            log.error(
                "This does not look like an AXML file. "
                "Declared filesize does not match real size: {} vs {}".format(
                    self.filesize, self.buff.size()
                )
            )
            self._valid = False
            return

        if self.filesize < self.buff.size():
            # The file can still be parsed up to the point where the chunk should end.
            self.axml_tampered = True
            log.warning(
                "Declared filesize ({}) is smaller than total file size ({}). "
                "Was something appended to the file? Trying to parse it anyways.".format(
                    self.filesize, self.buff.size()
                )
            )

        # Not that severe of an error, we have plenty files where this is not
        # set correctly
        if axml_header.type != const.RES_XML_TYPE:
            self.axml_tampered = True
            log.warning(
                "AXML file has an unusual resource type! "
                "Malware likes to to such stuff to anti androguard! "
                "But we try to parse it anyways. "
                "Resource Type: 0x{:04x}".format(axml_header.type)
            )

        # Now we parse the STRING POOL
        try:
            header = ARSCHeader(self.buff)
        except AssertionError as e:
            log.error("Error parsing resource header of string pool: %s", e)
            self._valid = False
            return

        if header.header_size != 0x1C:
            log.error(
                "This does not look like an AXML file. String chunk header "
                "size does not equal 28! header size = {}".format(
                    header.header_size
                )
            )
            self._valid = False
            return

        if header.type != const.RES_STRING_POOL_TYPE:
            log.error(
                "Expected String Pool header, got resource type 0x{:04x} "
                "instead".format(header.type)
            )
            self._valid = False
            return

        self.sb = StringBlock(self.buff, header)

        # Stores resource ID mappings, if any
        self.m_resourceIDs = []

        # Store a list of prefix/uri mappings encountered
        self.namespaces = []

    def is_valid(self):
        """
        Get the state of the AXMLPrinter.
        if an error happend somewhere in the process of parsing the file,
        this flag is set to False.
        """
        return self._valid

    def _reset(self):
        self.m_event = -1
        self.m_lineNumber = -1
        self.m_name = -1
        self.m_namespaceUri = -1
        self.m_attributes = []
        self.m_idAttribute = -1
        self.m_classAttribute = -1
        self.m_styleAttribute = -1

    def __next__(self):
        self._do_next()
        return self.m_event

    next = __next__  # For Python 2 compatibility

    def _do_next(self):
        if self.m_event == const.END_DOCUMENT:
            return

        self._reset()
        while self._valid:
            # Stop at the declared filesize or at the end of the file
            if self.buff.end() or self.buff.get_idx() == self.filesize:
                self.m_event = const.END_DOCUMENT
                break

            # Again, we read an ARSCHeader
            try:
                h = ARSCHeader(self.buff)
            except AssertionError as e:
                log.error("Error parsing resource header: %s", e)
                self._valid = False
                return

            # Special chunk: Resource Map. This chunk might be contained inside
            # the file, after the string pool.
            if h.type == const.RES_XML_RESOURCE_MAP_TYPE:
                log.debug("AXML contains a RESOURCE MAP")
                # Check size: < 8 bytes mean that the chunk is not complete
                # Should be aligned to 4 bytes.
                if h.size < 8 or (h.size % 4) != 0:
                    log.error("Invalid chunk size in chunk XML_RESOURCE_MAP")
                    self._valid = False
                    return

                for i in range((h.size - h.header_size) // 4):
                    self.m_resourceIDs.append(unpack('<L', self.buff.read(4))[0])

                continue

            # Parse now the XML chunks.
            # unknown chunk types might cause problems, but we can skip them!
            if h.type < const.RES_XML_FIRST_CHUNK_TYPE or h.type > const.RES_XML_LAST_CHUNK_TYPE:
                # h.size is the size of the whole chunk including the header.
                # We read already 8 bytes of the header, thus we need to
                # subtract them.
                log.error("Not a XML resource chunk type: 0x{:04x}. Skipping {} bytes".format(h.type, h.size))
                self.buff.set_idx(h.end)
                continue

            # Check that we read a correct header
            if h.header_size != 0x10:
                log.error(
                    "XML Resource Type Chunk header size does not match 16! "
                    "At chunk type 0x{:04x}, declared header size={}, "
                    "chunk size={}".format(h.type, h.header_size, h.size)
                )
                self.buff.seek(h.end)
                continue

            # Line Number of the source file, only used as meta information
            self.m_lineNumber, = unpack('<L', self.buff.read(4))

            # Comment_Index (usually 0xFFFFFFFF)
            self.m_comment_index, = unpack('<L', self.buff.read(4))

            if self.m_comment_index != 0xFFFFFFFF and h.type in [
                    const.RES_XML_START_NAMESPACE_TYPE,
                    const.RES_XML_END_NAMESPACE_TYPE]:
                log.warning("Unhandled Comment at namespace chunk: '{}'".format(
                    self.sb[self.m_comment_index])
                )

            if h.type == const.RES_XML_START_NAMESPACE_TYPE:
                prefix, = unpack('<L', self.buff.read(4))
                uri, = unpack('<L', self.buff.read(4))

                s_prefix = self.sb[prefix]
                s_uri = self.sb[uri]

                log.debug(
                    "Start of Namespace mapping: prefix "
                    "{}: '{}' --> uri {}: '{}'".format(
                        prefix, s_prefix, uri, s_uri
                    )
                )

                if s_uri == '':
                    log.warning("Namespace prefix '{}' resolves to empty URI. "
                                "This might be a packer.".format(s_prefix))

                if (prefix, uri) in self.namespaces:
                    log.info(
                        "Namespace mapping ({}, {}) already seen! "
                        "This is usually not a problem but could indicate "
                        "packers or broken AXML compilers.".format(prefix, uri))
                self.namespaces.append((prefix, uri))

                # We can continue with the next chunk, as we store the namespace
                # mappings for each tag
                continue

            if h.type == const.RES_XML_END_NAMESPACE_TYPE:
                # END_PREFIX contains again prefix and uri field
                prefix, = unpack('<L', self.buff.read(4))
                uri, = unpack('<L', self.buff.read(4))

                # We remove the last namespace mapping matching
                if (prefix, uri) in self.namespaces:
                    self.namespaces.remove((prefix, uri))
                else:
                    log.warning(
                        "Reached a NAMESPACE_END without having the namespace stored before? "
                        "Prefix ID: {}, URI ID: {}".format(prefix, uri)
                    )

                # We can continue with the next chunk, as we store the namespace
                # mappings for each tag
                continue

            # START_TAG is the start of a new tag.
            if h.type == const.RES_XML_START_ELEMENT_TYPE:
                # The TAG consists of some fields:
                # * (chunk_size, line_number, comment_index - we read before)
                # * namespace_uri
                # * name
                # * flags
                # * attribute_count
                # * class_attribute
                # After that, there are two lists of attributes, 20 bytes each

                # Namespace URI (String ID)
                self.m_namespaceUri, = unpack('<L', self.buff.read(4))
                # Name of the Tag (String ID)
                self.m_name, = unpack('<L', self.buff.read(4))
                # FIXME: Flags
                _ = self.buff.read(4)  # noqa
                # Attribute Count
                attributeCount, = unpack('<L', self.buff.read(4))
                # Class Attribute
                self.m_classAttribute, = unpack('<L', self.buff.read(4))

                self.m_idAttribute = (attributeCount >> 16) - 1
                self.m_attribute_count = attributeCount & 0xFFFF
                self.m_styleAttribute = (self.m_classAttribute >> 16) - 1
                self.m_classAttribute = (self.m_classAttribute & 0xFFFF) - 1

                # Now, we parse the attributes.
                # Each attribute has 5 fields of 4 byte
                for i in range(0, self.m_attribute_count * const.ATTRIBUTE_LENGHT):
                    # Each field is linearly parsed into the array
                    # Each Attribute contains:
                    # * Namespace URI (String ID)
                    # * Name (String ID)
                    # * Value
                    # * Type
                    # * Data
                    self.m_attributes.append(unpack('<L', self.buff.read(4))[0])

                # Then there are class_attributes
                for i in range(const.ATTRIBUTE_IX_VALUE_TYPE, len(self.m_attributes), const.ATTRIBUTE_LENGHT):
                    self.m_attributes[i] = self.m_attributes[i] >> 24

                self.m_event = const.START_TAG
                break

            if h.type == const.RES_XML_END_ELEMENT_TYPE:
                self.m_namespaceUri, = unpack('<L', self.buff.read(4))
                self.m_name, = unpack('<L', self.buff.read(4))

                self.m_event = const.END_TAG
                break

            if h.type == const.RES_XML_CDATA_TYPE:
                # The CDATA field is like an attribute.
                # It contains an index into the String pool
                # as well as a typed value.
                # usually, this typed value is set to UNDEFINED

                # ResStringPool_ref data --> uint32_t index
                self.m_name, = unpack('<L', self.buff.read(4))

                # Res_value typedData:
                # uint16_t size
                # uint8_t res0 -> always zero
                # uint8_t dataType
                # uint32_t data
                # For now, we ingore these values
                size, res0, dataType, data = unpack("<HBBL", self.buff.read(8))

                log.debug(
                    "found a CDATA Chunk: "
                    "index={: 6d}, size={: 4d}, res0={: 4d}, "
                    "dataType={: 4d}, data={: 4d}".format(
                        self.m_name, size, res0, dataType, data
                    )
                )

                self.m_event = const.TEXT
                break

            # Still here? Looks like we read an unknown XML header, try to skip it...
            log.warning("Unknown XML Chunk: 0x{:04x}, skipping {} bytes.".format(h.type, h.size))
            self.buff.set_idx(h.end)

    @property
    def name(self):
        """
        Return the String assosciated with the tag name
        """
        if self.m_name == -1 or (self.m_event != const.START_TAG and self.m_event != const.END_TAG):
            return u''

        return self.sb[self.m_name]

    @property
    def comment(self):
        """
        Return the comment at the current position or None if no comment is given

        This works only for Tags, as the comments of Namespaces are silently dropped.
        Currently, there is no way of retrieving comments of namespaces.
        """
        if self.m_comment_index == 0xFFFFFFFF:
            return None

        return self.sb[self.m_comment_index]

    @property
    def namespace(self):
        """
        Return the Namespace URI (if any) as a String for the current tag
        """
        if self.m_name == -1 or (self.m_event != const.START_TAG and self.m_event != const.END_TAG):
            return u''

        # No Namespace
        if self.m_namespaceUri == 0xFFFFFFFF:
            return u''

        return self.sb[self.m_namespaceUri]

    @property
    def nsmap(self):
        """
        Returns the current namespace mapping as a dictionary

        there are several problems with the map and we try to guess a few
        things here:

        1) a URI can be mapped by many prefixes, so it is to decide which one to take
        2) a prefix might map to an empty string (some packers)
        3) uri+prefix mappings might be included several times
        4) prefix might be empty
        """

        NSMAP = dict()
        # solve 3) by using a set
        for k, v in set(self.namespaces):
            s_prefix = self.sb[k]
            s_uri = self.sb[v]
            # Solve 2) & 4) by not including
            if s_uri != "" and s_prefix != "":
                # solve 1) by using the last one in the list
                NSMAP[s_prefix] = s_uri.strip()

        return NSMAP

    @property
    def text(self):
        """
        Return the String assosicated with the current text
        """
        if self.m_name == -1 or self.m_event != const.TEXT:
            return u''

        return self.sb[self.m_name]

    def getName(self):
        """
        Legacy only!
        use :py:attr:`~androguard.core.bytecodes.AXMLParser.name` instead
        """
        return self.name

    def getText(self):
        """
        Legacy only!
        use :py:attr:`~androguard.core.bytecodes.AXMLParser.text` instead
        """
        return self.text

    def getPrefix(self):
        """
        Legacy only!
        use :py:attr:`~androguard.core.bytecodes.AXMLParser.namespace` instead
        """
        return self.namespace

    def _get_attribute_offset(self, index):
        """
        Return the start inside the m_attributes array for a given attribute
        """
        if self.m_event != const.START_TAG:
            log.warning("Current event is not START_TAG.")

        offset = index * const.ATTRIBUTE_LENGHT
        if offset >= len(self.m_attributes):
            log.warning("Invalid attribute index")

        return offset

    def getAttributeCount(self):
        """
        Return the number of Attributes for a Tag
        or -1 if not in a tag
        """
        if self.m_event != const.START_TAG:
            return -1

        return self.m_attribute_count

    def getAttributeUri(self, index):
        """
        Returns the numeric ID for the namespace URI of an attribute
        """
        offset = self._get_attribute_offset(index)
        uri = self.m_attributes[offset + const.ATTRIBUTE_IX_NAMESPACE_URI]

        return uri

    def getAttributeNamespace(self, index):
        """
        Return the Namespace URI (if any) for the attribute
        """
        uri = self.getAttributeUri(index)

        # No Namespace
        if uri == 0xFFFFFFFF:
            return u''

        return self.sb[uri]

    def getAttributeName(self, index):
        """
        Returns the String which represents the attribute name
        """
        offset = self._get_attribute_offset(index)
        name = self.m_attributes[offset + const.ATTRIBUTE_IX_NAME]

        res = self.sb[name]
        # If the result is a (null) string, we need to look it up.
        if not res:
            attr = self.m_resourceIDs[name]
            if attr in public.SYSTEM_RESOURCES['attributes']['inverse']:
                res = 'android:' + public.SYSTEM_RESOURCES['attributes']['inverse'][attr]
            else:
                # Attach the HEX Number, so for multiple missing attributes we do not run
                # into problems.
                res = 'android:UNKNOWN_SYSTEM_ATTRIBUTE_{:08x}'.format(attr)

        return res

    def getAttributeValueType(self, index):
        """
        Return the type of the attribute at the given index

        :param index: index of the attribute
        """
        offset = self._get_attribute_offset(index)
        return self.m_attributes[offset + const.ATTRIBUTE_IX_VALUE_TYPE]

    def getAttributeValueData(self, index):
        """
        Return the data of the attribute at the given index

        :param index: index of the attribute
        """
        offset = self._get_attribute_offset(index)
        return self.m_attributes[offset + const.ATTRIBUTE_IX_VALUE_DATA]

    def getAttributeValue(self, index):
        """
        This function is only used to look up strings
        All other work is done by
        :func:`~androguard.core.bytecodes.axml.format_value`
        # FIXME should unite those functions
        :param index: index of the attribute
        :return:
        """
        offset = self._get_attribute_offset(index)
        valueType = self.m_attributes[offset + const.ATTRIBUTE_IX_VALUE_TYPE]
        if valueType == const.TYPE_STRING:
            valueString = self.m_attributes[offset + const.ATTRIBUTE_IX_VALUE_STRING]
            return self.sb[valueString]
        return u''
