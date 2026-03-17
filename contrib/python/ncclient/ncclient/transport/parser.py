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
import sys
import re

try:
    import selectors
except ImportError:
    import selectors2 as selectors

from io import BytesIO as StringIO
from xml.sax.handler import ContentHandler

from ncclient.transport.errors import NetconfFramingError
from ncclient.transport.session import NetconfBase
from ncclient.logging_ import SessionLoggerAdapter
from ncclient.operations.errors import OperationError
from ncclient.transport import SessionListener

import logging
logger = logging.getLogger("ncclient.transport.parser")


PORT_NETCONF_DEFAULT = 830
PORT_SSH_DEFAULT = 22

BUF_SIZE = 4096
# v1.0: RFC 4742
MSG_DELIM = "]]>]]>"
MSG_DELIM_LEN = len(MSG_DELIM)
# v1.1: RFC 6242
END_DELIM = '\n##\n'

TICK = 0.1

#
# Define delimiters for chunks and messages for netconf 1.1 chunk enoding.
# When matched:
#
# * result.group(0) will contain whole matched string
# * result.group(1) will contain the digit string for a chunk
# * result.group(2) will be defined if '##' found
#
RE_NC11_DELIM = re.compile(r'\n(?:#([0-9]+)|(##))\n')

def textify(buf):
    return buf.decode('UTF-8')


class SAXParserHandler(SessionListener):

    def __init__(self, session):
        self._session = session

    def callback(self, root, raw):
        if type(self._session.parser) == DefaultXMLParser:
            self._session.parser = self._session._device_handler.get_xml_parser(self._session)

    def errback(self, _):
        pass


class SAXFilterXMLNotFoundError(OperationError):
    def __init__(self, rpc_listener):
        self._listener = rpc_listener

    def __str__(self):
        return "SAX filter input xml not provided for listener: %s" % self._listener


class DefaultXMLParser:

    def __init__(self, session):
        """
        DOM Parser

        :param session: ssh session object
        """
        self._session = session
        self._parsing_pos10 = 0
        self.logger = SessionLoggerAdapter(logger, {'session': self._session})

    def parse(self, data):
        """
        parse incoming RPC response from networking device.

        :param data: incoming RPC data from device
        :return: None
        """
        if data:
            self._session._buffer.seek(0, os.SEEK_END)
            self._session._buffer.write(data)
            if self._session._base == NetconfBase.BASE_11:
                self._parse11()
            else:
                self._parse10()

    def _parse10(self):

        """Messages are delimited by MSG_DELIM. The buffer could have grown by
        a maximum of BUF_SIZE bytes everytime this method is called. Retains
        state across method calls and if a chunk has been read it will not be
        considered again."""

        self.logger.debug("parsing netconf v1.0")
        buf = self._session._buffer
        buf.seek(self._parsing_pos10)
        if MSG_DELIM in buf.read().decode('UTF-8'):
            buf.seek(0)
            msg, _, remaining = buf.read().decode('UTF-8').partition(MSG_DELIM)
            msg = msg.strip()
            self._session._dispatch_message(msg)
            self._session._buffer = StringIO()
            self._parsing_pos10 = 0
            if len(remaining.strip()) > 0:
                # There could be another entire message in the
                # buffer, so we should try to parse again.
                if type(self._session.parser) != DefaultXMLParser:
                    self.logger.debug('send remaining data to SAX parser')
                    self._session.parser.parse(remaining.encode())
                else:
                    self.logger.debug('Trying another round of parsing since there is still data')
                    self._session._buffer.write(remaining.encode())
                    self._parse10()
        else:
            # handle case that MSG_DELIM is split over two chunks
            self._parsing_pos10 = buf.tell() - MSG_DELIM_LEN
            if self._parsing_pos10 < 0:
                self._parsing_pos10 = 0

    def _parse11(self):

        """Messages are split into chunks. Chunks and messages are delimited
        by the regex #RE_NC11_DELIM defined earlier in this file. Each
        time we get called here either a chunk delimiter or an
        end-of-message delimiter should be found iff there is enough
        data. If there is not enough data, we will wait for more. If a
        delimiter is found in the wrong place, a #NetconfFramingError
        will be raised."""

        self.logger.debug("_parse11: starting")

        # suck in whole string that we have (this is what we will work on in
        # this function) and initialize a couple of useful values
        self._session._buffer.seek(0, os.SEEK_SET)
        data = self._session._buffer.getvalue()
        data_len = len(data)
        start = 0
        self.logger.debug('_parse11: working with buffer of %d bytes', data_len)
        while True and start < data_len:
            # match to see if we found at least some kind of delimiter
            self.logger.debug('_parse11: matching from %d bytes from start of buffer', start)
            re_result = RE_NC11_DELIM.match(data[start:].decode('utf-8', errors='ignore'))
            if not re_result:

                # not found any kind of delimiter just break; this should only
                # ever happen if we just have the first few characters of a
                # message such that we don't yet have a full delimiter
                self.logger.debug('_parse11: no delimiter found, buffer="%s"', data[start:].decode())
                break

            # save useful variables for reuse
            re_start = re_result.start()
            re_end = re_result.end()
            self.logger.debug('_parse11: regular expression start=%d, end=%d', re_start, re_end)

            # If the regex doesn't start at the beginning of the buffer,
            # we're in trouble, so throw an error
            if re_start != 0:
                raise NetconfFramingError('_parse11: delimiter not at start of match buffer', data[start:])

            if re_result.group(2):
                # we've found the end of the message, need to form up
                # whole message, save back remainder (if any) to buffer
                # and dispatch the message
                start += re_end
                message = ''.join(self._session._message_list)
                self._session._message_list = []
                self.logger.debug('_parse11: found end of message delimiter')
                self._session._dispatch_message(message)
                break

            elif re_result.group(1):
                # we've found a chunk delimiter, and group(1) is the digit
                # string that will tell us how many bytes past the end of
                # where it was found that we need to have available to
                # save the next chunk off
                self.logger.debug('_parse11: found chunk delimiter')
                digits = int(re_result.group(1))
                self.logger.debug('_parse11: chunk size %d bytes', digits)
                if (data_len-start) >= (re_end + digits):
                    # we have enough data for the chunk
                    fragment = textify(data[start+re_end:start+re_end+digits])
                    self._session._message_list.append(fragment)
                    start += re_end + digits
                    self.logger.debug('_parse11: appending %d bytes', digits)
                    self.logger.debug('_parse11: fragment = "%s"', fragment)
                else:
                    # we don't have enough bytes, just break out for now
                    # after updating start pointer to start of new chunk
                    start += re_start
                    self.logger.debug('_parse11: not enough data for chunk yet')
                    self.logger.debug('_parse11: setting start to %d', start)
                    break

        # Now out of the loop, need to see if we need to save back any content
        if start > 0:
            self.logger.debug(
                '_parse11: saving back rest of message after %d bytes, original size %d',
                start, data_len)
            self._session._buffer = StringIO(data[start:])
            if start < data_len:
                self.logger.debug('_parse11: still have data, may have another full message!')
                self._parse11()
        self.logger.debug('_parse11: ending')
