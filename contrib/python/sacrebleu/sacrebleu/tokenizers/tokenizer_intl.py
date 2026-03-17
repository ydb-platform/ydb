# -*- coding: utf-8 -*-

import re
import sys
import functools
import unicodedata

from .tokenizer_none import NoneTokenizer


class UnicodeRegex:
    """Ad-hoc hack to recognize all punctuation and symbols
    without depending on https://pypi.python.org/pypi/regex/."""

    @staticmethod
    def _property_chars(prefix):
        return ''.join(chr(x) for x in range(sys.maxunicode)
                       if unicodedata.category(chr(x)).startswith(prefix))

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def punctuation():
        return UnicodeRegex._property_chars('P')

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def nondigit_punct_re():
        return re.compile(r'([^\d])([' + UnicodeRegex.punctuation() + r'])')

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def punct_nondigit_re():
        return re.compile(r'([' + UnicodeRegex.punctuation() + r'])([^\d])')

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def symbol_re():
        return re.compile('([' + UnicodeRegex._property_chars('S') + '])')


class TokenizerV14International(NoneTokenizer):

    def signature(self):
        return 'intl'

    def __init__(self):
        self.nondigit_punct_re = UnicodeRegex.nondigit_punct_re()
        self.punct_nondigit_re = UnicodeRegex.punct_nondigit_re()
        self.symbol_re = UnicodeRegex.symbol_re()

    def __call__(self, line):
        r"""Tokenize a string following the official BLEU implementation.

        See https://github.com/moses-smt/mosesdecoder/blob/master/scripts/generic/mteval-v14.pl#L954-L983
        In our case, the input string is expected to be just one line
        and no HTML entities de-escaping is needed.
        So we just tokenize on punctuation and symbols,
        except when a punctuation is preceded and followed by a digit
        (e.g. a comma/dot as a thousand/decimal separator).

        Note that a number (e.g., a year) followed by a dot at the end of
        sentence is NOT tokenized, i.e. the dot stays with the number because
        `s/(\p{P})(\P{N})/ $1 $2/g` does not match this case (unless we add a
        space after each sentence). However, this error is already in the
        original mteval-v14.pl and we want to be consistent with it.
        The error is not present in the non-international version,
        which uses
        `$norm_text = " $norm_text "` (or `norm = " {} ".format(norm)` in Python).

        :param line: the input string
        :return: a list of tokens
        """
        line = self.nondigit_punct_re.sub(r'\1 \2 ', line)
        line = self.punct_nondigit_re.sub(r' \1 \2', line)
        line = self.symbol_re.sub(r' \1 ', line)
        return line.strip()
