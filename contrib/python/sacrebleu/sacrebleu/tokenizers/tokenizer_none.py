# -*- coding: utf-8 -*-

class NoneTokenizer:
    """A base dummy tokenizer to derive from."""

    def signature(self):
        """
        Returns a signature for the tokenizer.

        :return: signature string
        """
        return 'none'

    def __call__(self, line):
        """
        Tokenizes an input line with the tokenizer.

        :param line: a segment to tokenize
        :return: the tokenized line
        """
        return line
