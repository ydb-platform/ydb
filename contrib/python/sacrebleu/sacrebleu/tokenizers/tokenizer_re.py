import re

from .tokenizer_none import NoneTokenizer


class TokenizerRegexp(NoneTokenizer):

    def signature(self):
        return 're'

    def __init__(self):
        self._re = [
            # language-dependent part (assuming Western languages)
            (re.compile(r'([\{-\~\[-\` -\&\(-\+\:-\@\/])'), r' \1 '),
            # tokenize period and comma unless preceded by a digit
            (re.compile(r'([^0-9])([\.,])'), r'\1 \2 '),
            # tokenize period and comma unless followed by a digit
            (re.compile(r'([\.,])([^0-9])'), r' \1 \2'),
            # tokenize dash when preceded by a digit
            (re.compile(r'([0-9])(-)'), r'\1 \2 '),
            # one space only between words
            (re.compile(r'\s+'), r' '),
        ]

    def __call__(self, line):
        """Common post-processing tokenizer for `13a` and `zh` tokenizers.

        :param line: a segment to tokenize
        :return: the tokenized line
        """
        for (_re, repl) in self._re:
            line = _re.sub(repl, line)

        # no leading or trailing spaces
        return line.strip()
