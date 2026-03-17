# -*- coding: utf-8 -*-

from .tokenizer_none import NoneTokenizer


class TokenizerChar(NoneTokenizer):
    def signature(self):
        return 'char'

    def __init__(self):
        pass

    def __call__(self, line):
        """Tokenizes all the characters in the input line.

        :param line: a segment to tokenize
        :return: the tokenized line
        """
        return " ".join((char for char in line))
