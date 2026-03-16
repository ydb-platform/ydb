# -*- coding: utf-8 -*-

from .tokenizer_none import NoneTokenizer
from .tokenizer_re import TokenizerRegexp


class Tokenizer13a(NoneTokenizer):

    def signature(self):
        return '13a'

    def __init__(self):
        self._post_tokenizer = TokenizerRegexp()

    def __call__(self, line):
        """Tokenizes an input line using a relatively minimal tokenization
        that is however equivalent to mteval-v13a, used by WMT.

        :param line: a segment to tokenize
        :return: the tokenized line
        """

        # language-independent part:
        line = line.replace('<skipped>', '')
        line = line.replace('-\n', '')
        line = line.replace('\n', ' ')
        line = line.replace('&quot;', '"')
        line = line.replace('&amp;', '&')
        line = line.replace('&lt;', '<')
        line = line.replace('&gt;', '>')

        line = " {} ".format(line)
        return self._post_tokenizer(line)
