#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import io
import json
import os
import re
import unicodedata

import chardet

from textcode import pdf
from textcode import markup
from textcode import sfdb
from textcode import strings
import typecode

"""
Utilities to analyze text. Files are the input.
Once a file is read its output are unicode text lines.
All internal processing assumes unicode in and out.
"""

# Tracing flags
TRACE = False or os.environ.get('SCANCODE_DEBUG_TEXT_ANALYSIS', False)


# Tracing flags
def logger_debug(*args):
    pass


if TRACE:
    import logging
    import sys

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str) and a or repr(a) for a in args))


def numbered_text_lines(location, demarkup=False, plain_text=False):
    """
    Yield tuples of (line number, text line) from the file at `location`. Return
    an empty iterator if no text content is extractible. Text extraction is
    based on detected file type. Long lines are broken down in chunks, therefore
    two items can have the same line number.

    If `demarkup` is True, attempt to detect if a file contains HTML/XML-like
    markup and cleanup this markup.

    If `plain_text` is True treat the file as a plain text file and do not
    attempt to detect its type and extract it's content with special procedures.
    This is used mostly when loading license texts and rules.

    Note: For testing or building from strings, location can be a is a list of
    unicode line strings.
    """
    if not location:
        return iter([])

    if not isinstance(location, str):
        # not a path: wrap an iterator on location which should be a sequence
        # of lines
        if TRACE:
            logger_debug('numbered_text_lines:', 'location is not a file')
        return enumerate(iter(location), 1)

    if plain_text:
        if TRACE:
            logger_debug('numbered_text_lines:', 'plain_text')
        return enumerate(unicode_text_lines(location), 1)

    T = typecode.get_type(location)

    if TRACE:
        logger_debug('numbered_text_lines: T.filetype_file:', T.filetype_file)
        logger_debug('numbered_text_lines: T.is_text_with_long_lines:', T.is_text_with_long_lines)
        logger_debug('numbered_text_lines: T.is_binary:', T.is_binary)

    # TODO: we should have a command line to force digging inside binaries
    if not T.contains_text:
        return iter([])

    # Should we read this as some markup, pdf office doc, text or binary?
    if T.is_pdf and T.is_pdf_with_text:
        if TRACE:
            logger_debug('numbered_text_lines:', 'is_pdf')
        return enumerate(unicode_text_lines_from_pdf(location), 1)

    if T.filetype_file.startswith('Spline Font Database'):
        if TRACE:
            logger_debug('numbered_text_lines:', 'Spline Font Database')
        return enumerate((as_unicode(l) for l in sfdb.get_text_lines(location)), 1)

    # lightweight markup stripping support
    if demarkup and markup.is_markup(location):
        try:
            lines = list(enumerate(markup.demarkup(location), 1))
            if TRACE:
                logger_debug('numbered_text_lines:', 'demarkup')
            return lines
        except:
            # try again later with as plain text
            pass

    if T.is_js_map:
        try:
            lines = list(enumerate(js_map_sources_lines(location), 1))
            if TRACE:
                logger_debug('numbered_text_lines:', 'js_map')
            return lines
        except:
            # try again later with as plain text otherwise
            pass

    if T.is_text:
        numbered_lines = enumerate(unicode_text_lines(location), 1)
        # text with very long lines such minified JS, JS map files or large JSON
        if (not location.endswith('package.json')
            and (T.is_text_with_long_lines or T.is_compact_js
              or T.filetype_file == 'data' or 'locale' in location)):

            numbered_lines = break_numbered_unicode_text_lines(numbered_lines)
            if TRACE:
                logger_debug('numbered_text_lines:', 'break_numbered_unicode_text_lines')
        return numbered_lines

    # TODO: handle Office-like documents, RTF, etc
    # if T.is_doc:
    #     return unicode_text_lines_from_doc(location)

    # TODO: add support for "wide" UTF-16-like strings where each char is
    # followed by a zero as is often found in some Windows binaries. Do this for
    # binaries only. This is may conflicting  with "strings" extraction as
    # currently implemented
    if T.is_binary:
        # fall back to binary
        if TRACE:
            logger_debug('numbered_text_lines:', 'is_binary')

        return enumerate(unicode_text_lines_from_binary(location), 1)

    return iter([])


def unicode_text_lines_from_binary(location):
    """
    Return an iterable over unicode text lines extracted from a binary file at
    location.
    """
    T = typecode.get_type(location)
    if T.contains_text:
        for line in strings.strings_from_file(location):
            yield line


def unicode_text_lines_from_pdf(location):
    """
    Return an iterable over unicode text lines extracted from a pdf file at
    location.
    """
    for line in pdf.get_text_lines(location):
        yield as_unicode(line)


def break_numbered_unicode_text_lines(numbered_lines, split=u'([",\'])', max_len=200, chunk_len=30):
    """
    Yield text lines breaking long lines on `split` where numbered_lines is an
    iterator of (line number, line text).
    """
    splitter = re.compile(split).split
    for line_number, line in numbered_lines:
        if len(line) > max_len:
            # spli then reassemble in more reasonable chunks
            splitted = splitter(line)
            chunks = (splitted[i:i + chunk_len] for i in range(0, len(splitted), chunk_len))
            for chunk in chunks:
                full_chunk = u''.join(chunk)
                if full_chunk:
                    yield line_number, full_chunk
        else:
            yield line_number, line


def js_map_sources_lines(location):
    """
    Yield unicode text lines from the js.map or css.map file at `location`.
    Spec is at:
    https://docs.google.com/document/d/1U1RGAehQwRypUTovF1KRlpiOFze0b-_2gc6fAH0KY0k/edit
    The format is:
        {
            "version" : 3,
            "file": "out.js",
            "sourceRoot": "",
            "sources": ["foo.js", "bar.js"],
            "sourcesContent": [null, null],
            "names": ["src", "maps", "are", "fun"],
            "mappings": "A,AAAB;;ABCDE;"
        }
    We care only about the presence of these tags for detection: version, sources, sourcesContent.
    """
    with io.open(location, encoding='utf-8') as jsm:
        content = json.load(jsm)
        sources = content.get('sourcesContent', [])
        for entry in sources:
            for line in entry.splitlines():
                yield line


def as_unicode(line):
    """
    Return a unicode text line from a text line.
    Try to decode line as Unicode. Try first some default encodings,
    then attempt Unicode trans-literation and finally
    fall-back to ASCII strings extraction.

    TODO: Add file/magic detection, unicodedmanit/BS3/4
    """
    if isinstance(line, str):
        return remove_null_bytes(line)

    try:
        s = line.decode('UTF-8')
    except UnicodeDecodeError:
        try:
            # FIXME: latin-1 may never fail
            s = line.decode('LATIN-1')
        except UnicodeDecodeError:
            try:
                # Convert some byte string to ASCII characters as Unicode including
                # replacing accented characters with their non- accented NFKD
                # equivalent. Non ISO-Latin and non ASCII characters are stripped
                # from the output. Does not preserve the original length offsets.
                # For Unicode NFKD equivalence, see:
                # http://en.wikipedia.org/wiki/Unicode_equivalence
                s = unicodedata.normalize('NFKD', line).encode('ASCII')
            except UnicodeDecodeError:
                try:
                    enc = chardet.detect(line)['encoding']
                    s = str(line, enc)
                except UnicodeDecodeError:
                    # fall-back to strings extraction if all else fails
                    s = strings.string_from_string(s)
    return remove_null_bytes(s)


def remove_null_bytes(s):
    """
    Return a string replacing by a space all null bytes.

    There are some rare cases where we can have binary strings that are not
    caught early when detecting a file type, but only late at the line level.
    This help catch most of these cases.
    """
    return s.replace('\x00', ' ')


def remove_verbatim_cr_lf_tab_chars(s):
    """
    Return a string replacing by a space any verbatim but escaped line endings
    and tabs (such as a literal \n or \r \t).
    """
    if not s:
        return s
    return s.replace('\\r', ' ').replace('\\n', ' ').replace('\\t', ' ')


def unicode_text_lines(location):
    """
    Return an iterable over unicode text lines from a file at `location` if it
    contains text. Open the file as binary with universal new lines then try to
    decode each line as Unicode.
    """
    with open(location, 'rb') as f:
        for line in f.read().splitlines(True):
            yield remove_verbatim_cr_lf_tab_chars(as_unicode(line))


def unicode_text(location):
    """
    Return a string guaranteed to be unicode from the content of the file at
    location. The whole file content is returned at once, which may be a
    problem for very large files.
    """
    return u' '.join(unicode_text_lines(location))
