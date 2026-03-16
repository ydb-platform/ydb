# Copyright 2011-2015 Splunk, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""The **splunklib.results** module provides a streaming XML reader for Splunk
search results.

Splunk search results can be returned in a variety of formats including XML,
JSON, and CSV. To make it easier to stream search results in XML format, they
are returned as a stream of XML *fragments*, not as a single XML document. This
module supports incrementally reading one result record at a time from such a
result stream. This module also provides a friendly iterator-based interface for
accessing search results while avoiding buffering the result set, which can be
very large.

To use the reader, instantiate :class:`JSONResultsReader` on a search result stream
as follows:::

    reader = ResultsReader(result_stream)
    for item in reader:
        print(item)
    print "Results are a preview: %s" % reader.is_preview
"""

from __future__ import absolute_import

from io import BufferedReader, BytesIO

from splunklib import six

from splunklib.six import deprecated

try:
    import xml.etree.cElementTree as et
except:
    import xml.etree.ElementTree as et

from collections import OrderedDict
from json import loads as json_loads

try:
    from splunklib.six.moves import cStringIO as StringIO
except:
    from splunklib.six import StringIO

__all__ = [
    "ResultsReader",
    "Message",
    "JSONResultsReader"
]


class Message(object):
    """This class represents informational messages that Splunk interleaves in the results stream.

    ``Message`` takes two arguments: a string giving the message type (e.g., "DEBUG"), and
    a string giving the message itself.

    **Example**::

        m = Message("DEBUG", "There's something in that variable...")
    """

    def __init__(self, type_, message):
        self.type = type_
        self.message = message

    def __repr__(self):
        return "%s: %s" % (self.type, self.message)

    def __eq__(self, other):
        return (self.type, self.message) == (other.type, other.message)

    def __hash__(self):
        return hash((self.type, self.message))


class _ConcatenatedStream(object):
    """Lazily concatenate zero or more streams into a stream.

    As you read from the concatenated stream, you get characters from
    each stream passed to ``_ConcatenatedStream``, in order.

    **Example**::

        from StringIO import StringIO
        s = _ConcatenatedStream(StringIO("abc"), StringIO("def"))
        assert s.read() == "abcdef"
    """

    def __init__(self, *streams):
        self.streams = list(streams)

    def read(self, n=None):
        """Read at most *n* characters from this stream.

        If *n* is ``None``, return all available characters.
        """
        response = b""
        while len(self.streams) > 0 and (n is None or n > 0):
            txt = self.streams[0].read(n)
            response += txt
            if n is not None:
                n -= len(txt)
            if n is None or n > 0:
                del self.streams[0]
        return response


class _XMLDTDFilter(object):
    """Lazily remove all XML DTDs from a stream.

    All substrings matching the regular expression <?[^>]*> are
    removed in their entirety from the stream. No regular expressions
    are used, however, so everything still streams properly.

    **Example**::

        from StringIO import StringIO
        s = _XMLDTDFilter("<?xml abcd><element><?xml ...></element>")
        assert s.read() == "<element></element>"
    """

    def __init__(self, stream):
        self.stream = stream

    def read(self, n=None):
        """Read at most *n* characters from this stream.

        If *n* is ``None``, return all available characters.
        """
        response = b""
        while n is None or n > 0:
            c = self.stream.read(1)
            if c == b"":
                break
            elif c == b"<":
                c += self.stream.read(1)
                if c == b"<?":
                    while True:
                        q = self.stream.read(1)
                        if q == b">":
                            break
                else:
                    response += c
                    if n is not None:
                        n -= len(c)
            else:
                response += c
                if n is not None:
                    n -= 1
        return response


@deprecated("Use the JSONResultsReader function instead in conjuction with the 'output_mode' query param set to 'json'")
class ResultsReader(object):
    """This class returns dictionaries and Splunk messages from an XML results
    stream.

    ``ResultsReader`` is iterable, and returns a ``dict`` for results, or a
    :class:`Message` object for Splunk messages. This class has one field,
    ``is_preview``, which is ``True`` when the results are a preview from a
    running search, or ``False`` when the results are from a completed search.

    This function has no network activity other than what is implicit in the
    stream it operates on.

    :param `stream`: The stream to read from (any object that supports
        ``.read()``).

    **Example**::

        import results
        response = ... # the body of an HTTP response
        reader = results.ResultsReader(response)
        for result in reader:
            if isinstance(result, dict):
                print "Result: %s" % result
            elif isinstance(result, results.Message):
                print "Message: %s" % result
        print "is_preview = %s " % reader.is_preview
    """

    # Be sure to update the docstrings of client.Jobs.oneshot,
    # client.Job.results_preview and client.Job.results to match any
    # changes made to ResultsReader.
    #
    # This wouldn't be a class, just the _parse_results function below,
    # except that you cannot get the current generator inside the
    # function creating that generator. Thus it's all wrapped up for
    # the sake of one field.
    def __init__(self, stream):
        # The search/jobs/exports endpoint, when run with
        # earliest_time=rt and latest_time=rt streams a sequence of
        # XML documents, each containing a result, as opposed to one
        # results element containing lots of results. Python's XML
        # parsers are broken, and instead of reading one full document
        # and returning the stream that follows untouched, they
        # destroy the stream and throw an error. To get around this,
        # we remove all the DTD definitions inline, then wrap the
        # fragments in a fiction <doc> element to make the parser happy.
        stream = _XMLDTDFilter(stream)
        stream = _ConcatenatedStream(BytesIO(b"<doc>"), stream, BytesIO(b"</doc>"))
        self.is_preview = None
        self._gen = self._parse_results(stream)

    def __iter__(self):
        return self

    def next(self):
        return next(self._gen)

    __next__ = next

    def _parse_results(self, stream):
        """Parse results and messages out of *stream*."""
        result = None
        values = None
        try:
            for event, elem in et.iterparse(stream, events=('start', 'end')):
                if elem.tag == 'results' and event == 'start':
                    # The wrapper element is a <results preview="0|1">. We
                    # don't care about it except to tell is whether these
                    # are preview results, or the final results from the
                    # search.
                    is_preview = elem.attrib['preview'] == '1'
                    self.is_preview = is_preview
                if elem.tag == 'result':
                    if event == 'start':
                        result = OrderedDict()
                    elif event == 'end':
                        yield result
                        result = None
                        elem.clear()

                elif elem.tag == 'field' and result is not None:
                    # We need the 'result is not None' check because
                    # 'field' is also the element name in the <meta>
                    # header that gives field order, which is not what we
                    # want at all.
                    if event == 'start':
                        values = []
                    elif event == 'end':
                        field_name = elem.attrib['k']
                        if len(values) == 1:
                            result[field_name] = values[0]
                        else:
                            result[field_name] = values
                        # Calling .clear() is necessary to let the
                        # element be garbage collected. Otherwise
                        # arbitrarily large results sets will use
                        # arbitrarily large memory intead of
                        # streaming.
                        elem.clear()

                elif elem.tag in ('text', 'v') and event == 'end':
                    try:
                        text = "".join(elem.itertext())
                    except AttributeError:
                        # Assume we're running in Python < 2.7, before itertext() was added
                        # So we'll define it here

                        def __itertext(self):
                            tag = self.tag
                            if not isinstance(tag, six.string_types) and tag is not None:
                                return
                            if self.text:
                                yield self.text
                            for e in self:
                                for s in __itertext(e):
                                    yield s
                                if e.tail:
                                    yield e.tail

                        text = "".join(__itertext(elem))
                    values.append(text)
                    elem.clear()

                elif elem.tag == 'msg':
                    if event == 'start':
                        msg_type = elem.attrib['type']
                    elif event == 'end':
                        text = elem.text if elem.text is not None else ""
                        yield Message(msg_type, text)
                        elem.clear()
        except SyntaxError as pe:
            # This is here to handle the same incorrect return from
            # splunk that is described in __init__.
            if 'no element found' in pe.msg:
                return
            else:
                raise


class JSONResultsReader(object):
    """This class returns dictionaries and Splunk messages from a JSON results
    stream.
    ``JSONResultsReader`` is iterable, and returns a ``dict`` for results, or a
    :class:`Message` object for Splunk messages. This class has one field,
    ``is_preview``, which is ``True`` when the results are a preview from a
    running search, or ``False`` when the results are from a completed search.

    This function has no network activity other than what is implicit in the
    stream it operates on.

    :param `stream`: The stream to read from (any object that supports``.read()``).

    **Example**::

        import results
        response = ... # the body of an HTTP response
        reader = results.JSONResultsReader(response)
        for result in reader:
            if isinstance(result, dict):
                print "Result: %s" % result
            elif isinstance(result, results.Message):
                print "Message: %s" % result
        print "is_preview = %s " % reader.is_preview
    """

    # Be sure to update the docstrings of client.Jobs.oneshot,
    # client.Job.results_preview and client.Job.results to match any
    # changes made to JSONResultsReader.
    #
    # This wouldn't be a class, just the _parse_results function below,
    # except that you cannot get the current generator inside the
    # function creating that generator. Thus it's all wrapped up for
    # the sake of one field.
    def __init__(self, stream):
        # The search/jobs/exports endpoint, when run with
        # earliest_time=rt and latest_time=rt, output_mode=json, streams a sequence of
        # JSON documents, each containing a result, as opposed to one
        # results element containing lots of results.
        stream = BufferedReader(stream)
        self.is_preview = None
        self._gen = self._parse_results(stream)

    def __iter__(self):
        return self

    def next(self):
        return next(self._gen)

    __next__ = next

    def _parse_results(self, stream):
        """Parse results and messages out of *stream*."""
        for line in stream.readlines():
            strip_line = line.strip()
            if strip_line.__len__() == 0: continue
            parsed_line = json_loads(strip_line)
            if "preview" in parsed_line:
                self.is_preview = parsed_line["preview"]
            if "messages" in parsed_line and parsed_line["messages"].__len__() > 0:
                for message in parsed_line["messages"]:
                    msg_type = message.get("type", "Unknown Message Type")
                    text = message.get("text")
                yield Message(msg_type, text)
            if "result" in parsed_line:
                yield parsed_line["result"]
            if "results" in parsed_line:
                for result in parsed_line["results"]:
                    yield result
