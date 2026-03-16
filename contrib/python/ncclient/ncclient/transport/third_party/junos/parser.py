# Copyright 2018 Nitin Kumar

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

from threading import Lock

import difflib

from xml.sax.handler import ContentHandler
from lxml import etree
from lxml.builder import E
from xml.sax._exceptions import SAXParseException
from xml.sax import make_parser

from ncclient.transport.parser import DefaultXMLParser
from ncclient.operations import rpc
from ncclient.transport.parser import SAXFilterXMLNotFoundError
from ncclient.transport.parser import MSG_DELIM, MSG_DELIM_LEN
from ncclient.operations.errors import OperationError

import logging
logger = logging.getLogger("ncclient.transport.third_party.junos.parser")

from ncclient.xml_ import BASE_NS_1_0

RPC_REPLY_END_TAG = "</rpc-reply>"
RFC_RPC_REPLY_END_TAG = "</nc:rpc-reply>"


class JunosXMLParser(DefaultXMLParser):
    def __init__(self, session):
        super(JunosXMLParser, self).__init__(session)
        self._session = session
        self.sax_parser = make_parser()
        self.sax_parser.setContentHandler(SAXParser(session))

    def parse(self, data):
        try:
            self.sax_parser.feed(data)
        except SAXParseException:
            self._delimiter_check(data)
        except SAXFilterXMLNotFoundError:
            self.logger.debug('Missing SAX filter_xml. Switching from sax to dom parsing')
            self._session.parser = DefaultXMLParser(self._session)
            if not isinstance(data, bytes):
                data = str.encode(data)
            self._session._buffer.write(data)
        finally:
            self._parse10()

    def _delimiter_check(self, data):
        """
        SAX parser throws SAXParseException exception, if there is extra data
        after MSG_DELIM

        :param data: content read by select loop
        :return: None
        """
        data = data.decode('UTF-8')
        if MSG_DELIM in data:
            # need to feed extra data after MSG_DELIM
            msg, delim, remaining = data.partition(MSG_DELIM)
            self._session._buffer.seek(0, os.SEEK_END)
            self._session._buffer.write(delim.encode())
            if remaining.strip() != '':
                self._session._buffer.write(remaining.encode())
            # we need to renew parser, as old parser is gone.
            self.sax_parser = make_parser()
            self.sax_parser.setContentHandler(SAXParser(self._session))
        elif RPC_REPLY_END_TAG in data or RFC_RPC_REPLY_END_TAG in data:
            tag = RPC_REPLY_END_TAG if RPC_REPLY_END_TAG in data else \
                RFC_RPC_REPLY_END_TAG
            logger.warning("Check for rpc reply end tag within data received: %s" % data)
            msg, delim, remaining = data.partition(tag)
            self._session._buffer.seek(0, os.SEEK_END)
            self._session._buffer.write(remaining.encode())
        else:
            logger.warning("Check if end delimiter is split within data received: %s" % data)
            # When data is "-reply/>]]>" or "]]>"
            # Data is not full MSG_DELIM, So check if last rpc reply is complete.
            # if then, wait for next iteration of data and do a recursive call to
            # _delimiter_check for MSG_DELIM check
            buf = self._session._buffer
            buf.seek(buf.tell() - len(RFC_RPC_REPLY_END_TAG) - MSG_DELIM_LEN)
            rpc_response_last_msg = buf.read().decode('UTF-8').replace('\n', '')
            if RPC_REPLY_END_TAG in rpc_response_last_msg or \
                    RFC_RPC_REPLY_END_TAG in rpc_response_last_msg:
                tag = RPC_REPLY_END_TAG if RPC_REPLY_END_TAG in \
                                           rpc_response_last_msg else \
                    RFC_RPC_REPLY_END_TAG
                # rpc_response_last_msg and data can be overlapping
                match_obj = difflib.SequenceMatcher(None, rpc_response_last_msg,
                                                    data).get_matching_blocks()
                if match_obj:
                    # 0 means second string match start from beginning, hence
                    # there is a overlap
                    if match_obj[0].b == 0:
                        # matching char are of match_obj[0].size
                        self._delimiter_check((rpc_response_last_msg +
                                               data[match_obj[0].size:]).encode())
                    else:
                        data = rpc_response_last_msg+data
                        if MSG_DELIM in data:
                            # there can be residual end delimiter chars in buffer.
                            # as first if condition will add full delimiter, so clean
                            # it off
                            clean_up = len(rpc_response_last_msg) - (
                                    rpc_response_last_msg.find(tag) + len(tag))
                            self._session._buffer.truncate(buf.tell() - clean_up)
                            self._delimiter_check(data.encode())
                        else:
                            self._delimiter_check((rpc_response_last_msg + data).encode())


def __dict_replace(s, d):
    """Replace substrings of a string using a dictionary."""
    for key, value in d.items():
        s = s.replace(key, value)
    return s


def _get_sax_parser_root(xml):
    """
    This function does some validation and rule check of xmlstring
    :param xml: string or object to be used in parsing reply
    :return: lxml object
    """
    if isinstance(xml, etree._Element):
        root = xml
    else:
        root = etree.fromstring(xml)
    return root


def escape(data, entities={}):
    """Escape &, <, and > in a string of data.

    You can escape other strings of data by passing a dictionary as
    the optional entities parameter.  The keys and values must all be
    strings; each key will be replaced with its corresponding value.
    """

    # must do ampersand first
    data = data.replace("&", "&amp;")
    data = data.replace(">", "&gt;")
    data = data.replace("<", "&lt;")
    if entities:
        data = __dict_replace(data, entities)
    return data


def quoteattr(data, entities={}):
    """Escape and quote an attribute value.

    Escape &, <, and > in a string of data, then quote it for use as
    an attribute value.  The \" character will be escaped as well, if
    necessary.

    You can escape other strings of data by passing a dictionary as
    the optional entities parameter.  The keys and values must all be
    strings; each key will be replaced with its corresponding value.
    """
    # entities = entities.copy()
    entities.update({'\n': '&#10;', '\r': '&#13;', '\t':'&#9;'})
    data = escape(data, entities)
    if '"' in data:
        if "'" in data:
            data = '"%s"' % data.replace('"', "&quot;")
        else:
            data = "'%s'" % data
    else:
        data = '"%s"' % data
    return data


class SAXParser(ContentHandler):
    def __init__(self, session):
        ContentHandler.__init__(self)
        self._currenttag = None
        self._ignoretag = None
        self._defaulttags = []
        self._session = session
        self._validate_reply_and_sax_tag = False
        self._lock = Lock()
        self.nc_namespace = None

    def startElement(self, tag, attributes):
        if tag in ['rpc-reply', 'nc:rpc-reply']:
            if tag == 'nc:rpc-reply':
                self.nc_namespace = BASE_NS_1_0
            # in case last rpc called used sax parsing and error'd out
            # without resetting use_filer in endElement rpc-reply check
            with self._lock:
                listeners = list(self._session._listeners)
            rpc_reply_listener = [i for i in listeners if
                                 isinstance(i, rpc.RPCReplyListener)]
            rpc_msg_id = attributes._attrs['message-id']
            if rpc_msg_id in rpc_reply_listener[0]._id2rpc:
                rpc_reply_handler = rpc_reply_listener[0]._id2rpc[rpc_msg_id]
                if hasattr(rpc_reply_handler, '_filter_xml') and \
                        rpc_reply_handler._filter_xml is not None:
                    self._cur = self._root = _get_sax_parser_root(
                        rpc_reply_handler._filter_xml)
                else:
                    raise SAXFilterXMLNotFoundError(rpc_reply_handler)
            else:
                raise OperationError("Unknown 'message-id': %s" % rpc_msg_id)
        if self._ignoretag is not None:
            return

        if self._cur == self._root and self._cur.tag == tag:
            node = self._root
        else:
            node = self._cur.find(tag, namespaces={"nc": self.nc_namespace})

        if self._validate_reply_and_sax_tag:
            if tag != self._root.tag:
                self._write_buffer(tag, format_str='<{}>\n')
                self._cur = E(tag, self._cur)
            else:
                self._write_buffer(tag, format_str='<{}{}>', **attributes)
                self._cur = node
                self._currenttag = tag
            self._validate_reply_and_sax_tag = False
            self._defaulttags.append(tag)
        elif node is not None:
            self._write_buffer(tag, format_str='<{}{}>', **attributes)
            self._cur = node
            self._currenttag = tag
        elif tag in ['rpc-reply', 'nc:rpc-reply']:
            self._write_buffer(tag, format_str='<{}{}>', **attributes)
            self._defaulttags.append(tag)
            self._validate_reply_and_sax_tag = True
        else:
            self._currenttag = None
            self._ignoretag = tag

    def endElement(self, tag):
        if self._ignoretag == tag:
            self._ignoretag = None

        if tag in self._defaulttags:
            self._write_buffer(tag, format_str='</{}>\n')
        elif self._cur.tag == tag:
            self._write_buffer(tag, format_str='</{}>\n')
            self._cur = self._cur.getparent()

        self._currenttag = None

    def characters(self, content):
        if self._currenttag is not None:
            self._write_buffer(content, format_str='{}')

    def _write_buffer(self, content, format_str, **kwargs):
        # print(content, format_str, kwargs)
        self._session._buffer.seek(0, os.SEEK_END)
        attrs = ''
        for (name, value) in kwargs.items():
            attr = ' {}={}'.format(name, quoteattr(value))
            attrs = attrs + attr
        data = format_str.format(escape(content), attrs)
        self._session._buffer.write(str.encode(data))
