# -*- coding: utf-8 -*-
#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from itertools import islice
from binascii import crc32
import re

from licensedcode.stopwords import STOPWORDS
from textcode.analysis import numbered_text_lines


"""
Utilities to break texts in lines and tokens (aka. words) with specialized version
for queries and rules texts.
"""


def query_lines(location=None, query_string=None, strip=True):
    """
    Return an iterable of tuples (line number, text line) given a file at
    `location` or a `query string`. Include empty lines.
    """
    # TODO: OPTIMIZE: tokenizing line by line may be rather slow
    # we could instead get lines and tokens at once in a batch?
    numbered_lines = []
    if location:
        numbered_lines = numbered_text_lines(location, demarkup=False)
    elif query_string:
        if strip:
            keepends = False
        else:
            keepends = True
        numbered_lines = enumerate(query_string.splitlines(keepends), 1)

    for line_number, line in numbered_lines:
        if strip:
            yield line_number, line.strip()
        else:
            yield line_number, line.rstrip('\n') + '\n'


# Split on whitespace and punctuations: keep only characters and numbers and +
# when in the middle or end of a word. Keeping the trailing + is important for
# licenses name such as GPL2+
query_pattern = '[^_\\W]+\\+?[^_\\W]*'
word_splitter = re.compile(query_pattern, re.UNICODE).findall


def index_tokenizer(text, stopwords=STOPWORDS):
    """
    Return an iterable of tokens from a unicode query text. Ignore words that
    exist as lowercase in the `stopwords` set.

    For example::
    >>> list(index_tokenizer(''))
    []
    >>> x = list(index_tokenizer('some Text with   spAces! + _ -'))
    >>> assert x == ['some', 'text', 'with', 'spaces']

    >>> x = list(index_tokenizer('{{}some }}Text with   spAces! + _ -'))
    >>> assert x == ['some', 'text', 'with', 'spaces']

    >>> x = list(index_tokenizer('{{Hi}}some {{}}Text with{{noth+-_!@ing}}   {{junk}}spAces! + _ -{{}}'))
    >>> assert x == ['hi', 'some', 'text', 'with', 'noth+', 'ing', 'junk', 'spaces']

    >>> stops = set(['quot', 'lt', 'gt'])
    >>> x = list(index_tokenizer('some &quot&lt markup &gt&quot', stopwords=stops))
    >>> assert x == ['some', 'markup']
    """
    if not text:
        return []
    words = word_splitter(text.lower())
    return (token for token in words if token and token not in stopwords)


def query_tokenizer(text):
    """
    Return an iterable of tokens from a unicode query text. Do not ignore stop
    words. They are handled at a later stage in a query.

    For example::
    >>> list(query_tokenizer(''))
    []
    >>> x = list(query_tokenizer('some Text with   spAces! + _ -'))
    >>> assert x == ['some', 'text', 'with', 'spaces']

    >>> x = list(query_tokenizer('{{}some }}Text with   spAces! + _ -'))
    >>> assert x == ['some', 'text', 'with', 'spaces']

    >>> x = list(query_tokenizer('{{Hi}}some {{}}Text with{{noth+-_!@ing}}   {{junk}}spAces! + _ -{{}}'))
    >>> assert x == ['hi', 'some', 'text', 'with', 'noth+', 'ing', 'junk', 'spaces']
    """
    if not text:
        return []
    words = word_splitter(text.lower())
    return (token for token in words if token)


# Alternate pattern which is the opposite of query_pattern used for
# matched text collection
not_query_pattern = '[_\\W\\s\\+]+[_\\W\\s]?'

# collect tokens and non-token texts in two different groups
_text_capture_pattern = (
    '(?P<token>' +
        query_pattern +
    ')' +
    '|' +
    '(?P<punct>' +
        not_query_pattern +
    ')'
)
tokens_and_non_tokens = re.compile(_text_capture_pattern, re.UNICODE).finditer


def matched_query_text_tokenizer(text):
    """
    Return an iterable of tokens and non-tokens punctuation from a unicode query
    text keeping everything (including punctuations, line endings, etc.)
    The returned iterable contains 2-tuples of:
    - True if the string is a text token or False if this is not
      (such as punctuation, spaces, etc).
    - the corresponding string.
    This is used to reconstruct the matched query text for reporting.
    """
    if not text:
        return
    for match in tokens_and_non_tokens(text):
        if match:
            mgd = match.groupdict()
            token = mgd.get('token')
            punct = mgd.get('punct')
            if token:
                yield True, token
            elif punct:
                yield False, punct
            else:
                # this should never happen
                raise Exception('Internal error in matched_query_text_tokenizer')


def ngrams(iterable, ngram_length):
    """
    Return an iterable of ngrams of length `ngram_length` given an `iterable`.
    Each ngram is a tuple of `ngram_length` items.

    The returned iterable is empty if the input iterable contains less than
    `ngram_length` items.

    Note: this is a fairly arcane but optimized way to compute ngrams.

    For example:
    >>> list(ngrams([1,2,3,4,5], 2))
    [(1, 2), (2, 3), (3, 4), (4, 5)]

    >>> list(ngrams([1,2,3,4,5], 4))
    [(1, 2, 3, 4), (2, 3, 4, 5)]

    >>> list(ngrams([1,2,3,4], 2))
    [(1, 2), (2, 3), (3, 4)]

    >>> list(ngrams([1,2,3], 2))
    [(1, 2), (2, 3)]

    >>> list(ngrams([1,2], 2))
    [(1, 2)]

    >>> list(ngrams([1], 2))
    []

    This also works with arrays or tuples:

    >>> from array import array
    >>> list(ngrams(array('h', [1,2,3,4,5]), 2))
    [(1, 2), (2, 3), (3, 4), (4, 5)]

    >>> list(ngrams(tuple([1,2,3,4,5]), 2))
    [(1, 2), (2, 3), (3, 4), (4, 5)]
    """
    return zip(*(islice(iterable, i, None) for i in range(ngram_length)))


def select_ngrams(ngrams, with_pos=False):
    """
    Return an iterable as a subset of a sequence of ngrams using the hailstorm
    algorithm. If `with_pos` is True also include the starting position for the
    ngram in the original sequence.

    Definition from the paper: http://www2009.eprints.org/7/1/p61.pdf

      The algorithm first fingerprints every token and then selects a shingle s
      if the minimum fingerprint value of all k tokens in s occurs at the first
      or the last position of s (and potentially also in between). Due to the
      probabilistic properties of Rabin fingerprints the probability that a
      shingle is chosen is 2/k if all tokens in the shingle are different.

    For example:
    >>> list(select_ngrams([(2, 1, 3), (1, 1, 3), (5, 1, 3), (2, 6, 1), (7, 3, 4)]))
    [(2, 1, 3), (1, 1, 3), (5, 1, 3), (2, 6, 1), (7, 3, 4)]

    Positions can also be included. In this case, tuple of (pos, ngram) are returned:
    >>> list(select_ngrams([(2, 1, 3), (1, 1, 3), (5, 1, 3), (2, 6, 1), (7, 3, 4)], with_pos=True))
    [(0, (2, 1, 3)), (1, (1, 1, 3)), (2, (5, 1, 3)), (3, (2, 6, 1)), (4, (7, 3, 4))]

    This works also from a generator:
    >>> list(select_ngrams(x for x in [(2, 1, 3), (1, 1, 3), (5, 1, 3), (2, 6, 1), (7, 3, 4)]))
    [(2, 1, 3), (1, 1, 3), (5, 1, 3), (2, 6, 1), (7, 3, 4)]
    """
    last = None
    for pos, ngram in enumerate(ngrams):
        # FIXME: use a proper hash
        nghs = []
        for ng in ngram:
            if isinstance(ng, str):
                ng = bytearray(ng, encoding='utf-8')
            else:
                ng = bytearray(str(ng).encode('utf-8'))
            nghs.append(crc32(ng) & 0xffffffff)
        min_hash = min(nghs)
        if with_pos:
            ngram = (pos, ngram,)
        if min_hash in (nghs[0], nghs[-1]):
            yield ngram
            last = ngram
        else:
            # always yield the first or last ngram too.
            if pos == 0:
                yield ngram
                last = ngram
    if last != ngram:
        yield ngram
