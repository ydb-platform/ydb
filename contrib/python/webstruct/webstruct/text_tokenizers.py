# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
import re
import collections

TextToken = collections.namedtuple('TextToken', 'chars, position, length')


class WordTokenizer(object):
    r"""This tokenizer is copy-pasted version of TreebankWordTokenizer
    that doesn't split on @ and ':' symbols and doesn't split contractions.
    It supports span_tokenize(in terms of nltk tokenizers) method - :meth:`segment_words`::

    >>> s = '''Good muffins cost $3.88\nin New York. Email: muffins@gmail.com'''
    >>> WordTokenizer().segment_words(s)
    [TextToken(chars='Good', position=0, length=4),
     TextToken(chars='muffins', position=5, length=7),
     TextToken(chars='cost', position=13, length=4),
     TextToken(chars='$', position=18, length=1),
     TextToken(chars='3.88', position=19, length=4),
     TextToken(chars='in', position=24, length=2),
     TextToken(chars='New', position=27, length=3),
     TextToken(chars='York.', position=31, length=5),
     TextToken(chars='Email:', position=37, length=6),
     TextToken(chars='muffins@gmail.com', position=44, length=17)]

    >>> s = '''Shelbourne Road,'''
    >>> WordTokenizer().segment_words(s)
    [TextToken(chars='Shelbourne', position=0, length=10),
     TextToken(chars='Road', position=11, length=4),
     TextToken(chars=',', position=15, length=1)]

    >>> s = '''population of 100,000'''
    >>> WordTokenizer().segment_words(s)
    [TextToken(chars='population', position=0, length=10),
     TextToken(chars='of', position=11, length=2),
     TextToken(chars='100,000', position=14, length=7)]

    >>> s = '''Hello|World'''
    >>> WordTokenizer().segment_words(s)
    [TextToken(chars='Hello', position=0, length=5),
     TextToken(chars='|', position=5, length=1),
     TextToken(chars='World', position=6, length=5)]

    >>> s2 = '"We beat some pretty good teams to get here," Slocum said.'
    >>> WordTokenizer().segment_words(s2)  # doctest: +NORMALIZE_WHITESPACE
    [TextToken(chars='``', position=0, length=1),
     TextToken(chars='We', position=1, length=2),
     TextToken(chars='beat', position=4, length=4),
     TextToken(chars='some', position=9, length=4),
     TextToken(chars='pretty', position=14, length=6),
     TextToken(chars='good', position=21, length=4),
     TextToken(chars='teams', position=26, length=5),
     TextToken(chars='to', position=32, length=2),
     TextToken(chars='get', position=35, length=3),
     TextToken(chars='here', position=39, length=4),
     TextToken(chars=',', position=43, length=1),
     TextToken(chars="''", position=44, length=1),
     TextToken(chars='Slocum', position=46, length=6),
     TextToken(chars='said', position=53, length=4),
     TextToken(chars='.', position=57, length=1)]
    >>> s3 = '''Well, we couldn't have this predictable,
    ... cliche-ridden, \"Touched by an
    ... Angel\" (a show creator John Masius
    ... worked on) wanna-be if she didn't.'''
    >>> WordTokenizer().segment_words(s3)  # doctest: +NORMALIZE_WHITESPACE
    [TextToken(chars='Well', position=0, length=4),
     TextToken(chars=',', position=4, length=1),
     TextToken(chars='we', position=6, length=2),
     TextToken(chars="couldn't", position=9, length=8),
     TextToken(chars='have', position=18, length=4),
     TextToken(chars='this', position=23, length=4),
     TextToken(chars='predictable', position=28, length=11),
     TextToken(chars=',', position=39, length=1),
     TextToken(chars='cliche-ridden', position=41, length=13),
     TextToken(chars=',', position=54, length=1),
     TextToken(chars='``', position=56, length=1),
     TextToken(chars='Touched', position=57, length=7),
     TextToken(chars='by', position=65, length=2),
     TextToken(chars='an', position=68, length=2),
     TextToken(chars='Angel', position=71, length=5),
     TextToken(chars="''", position=76, length=1),
     TextToken(chars='(', position=78, length=1),
     TextToken(chars='a', position=79, length=1),
     TextToken(chars='show', position=81, length=4),
     TextToken(chars='creator', position=86, length=7),
     TextToken(chars='John', position=94, length=4),
     TextToken(chars='Masius', position=99, length=6),
     TextToken(chars='worked', position=106, length=6),
     TextToken(chars='on', position=113, length=2),
     TextToken(chars=')', position=115, length=1),
     TextToken(chars='wanna-be', position=117, length=8),
     TextToken(chars='if', position=126, length=2),
     TextToken(chars='she', position=129, length=3),
     TextToken(chars="didn't", position=133, length=6),
     TextToken(chars='.', position=139, length=1)]

    >>> WordTokenizer().segment_words('"')
    [TextToken(chars='``', position=0, length=1)]

    >>> WordTokenizer().segment_words('" a')
    [TextToken(chars='``', position=0, length=1),
     TextToken(chars='a', position=2, length=1)]

    >>> WordTokenizer().segment_words('["a')
    [TextToken(chars='[', position=0, length=1),
     TextToken(chars='``', position=1, length=1),
     TextToken(chars='a', position=2, length=1)]

    Some issues:

    >>> WordTokenizer().segment_words("Copyright © 2014 Foo Bar and Buzz Spam. All Rights Reserved.")
    [TextToken(chars='Copyright', position=0, length=9),
     TextToken(chars=u'\xa9', position=10, length=1),
     TextToken(chars='2014', position=12, length=4),
     TextToken(chars='Foo', position=17, length=3),
     TextToken(chars='Bar', position=21, length=3),
     TextToken(chars='and', position=25, length=3),
     TextToken(chars='Buzz', position=29, length=4),
     TextToken(chars='Spam.', position=34, length=5),
     TextToken(chars='All', position=40, length=3),
     TextToken(chars='Rights', position=44, length=6),
     TextToken(chars='Reserved', position=51, length=8),
     TextToken(chars='.', position=59, length=1)]

    """

    # regex, token
    # if token is None - regex match group is taken
    rules = [
        (re.compile(r'\s+', re.UNICODE), ''),
        (re.compile(r'“'), "``"),
        (re.compile(r'["”]'), "''"),
        (re.compile(r'``'), None),
        (re.compile(r'…|\.\.\.'), '...'),
        (re.compile(r'--'), None),
        (re.compile(r',(?=\D|$)'), None),
        (re.compile(r'\.$'), None),
        (re.compile(r'[;#$£%&|!?[\](){}<>]'), None),
        (re.compile(r"'(?=\s)|''", re.UNICODE), None),
    ]

    open_quotes = re.compile(r'(^|[\s(\[{<])"')

    def _segment_words(self, text):
        # this one cannot be placed in the loop of internal function because it requires
        # position check (beginning of the string) or previous char value
        start = 0
        for quote in self.open_quotes.finditer(text):
            quote_pos = quote.end() - 1
            for token in self._segment_words_nonquote(text[start:quote_pos]):
                yield TextToken(chars=token.chars,
                                position=token.position + start,
                                length=token.length)
            yield TextToken(chars='``', position=quote_pos, length=1)
            start = quote.end()

        for token in self._segment_words_nonquote(text[start:]):
            yield TextToken(chars=token.chars,
                            position=token.position + start,
                            length=token.length)


    def _segment_words_nonquote(self, text):
        i = 0
        token_start = 0
        while 1:
            if i >= len(text):
                yield TextToken(chars=text[token_start:],
                                position=token_start,
                                length=len(text) - token_start)
                break
            shift = 1
            partial_text = text[i:]
            for regex, token in self.rules:
                match = regex.match(partial_text)
                if match:
                    yield TextToken(chars=text[token_start:i],
                                    position=token_start,
                                    length=i - token_start)
                    shift = match.end() - match.start()
                    token_start = i + shift
                    if token is None:
                        yield TextToken(chars=match.group(),
                                        position=i + match.start(),
                                        length=shift)
                    else:
                        yield TextToken(chars=token,
                                        position=i + match.start(),
                                        length=shift)
                    break
            i += shift

    def segment_words(self, text):
        return [t for t in self._segment_words(text) if t.chars]

    def tokenize(self, text):
        return [t.chars for t in self.segment_words(text)]


class DefaultTokenizer(WordTokenizer):
    def segment_words(self, text):
        tokens = super(DefaultTokenizer, self).segment_words(text)
        # remove standalone commas and semicolons
        # as they broke tag sets,
        # e.g. PERSON->FUNCTION in case "PERSON, FUNCTION"

        # but it has negative consequences, e.g.
        # etalon:    [PER-B, PER-I, FUNC-B]
        # predicted: [PER-B, PER-I, PER-I ]
        # because we removed punctuation

        # FIXME: remove as token, but save as feature left/right_punct:","
        return [t for t in tokens if t.chars not in {',', ';'}]


tokenize = DefaultTokenizer().segment_words
