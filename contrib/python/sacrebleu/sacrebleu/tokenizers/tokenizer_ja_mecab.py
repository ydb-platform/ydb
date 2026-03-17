# -*- coding: utf-8 -*-
try:
    import MeCab
    import ipadic
except ImportError:
    # Don't fail until the tokenizer is actually used
    MeCab = None

from .tokenizer_none import NoneTokenizer

FAIL_MESSAGE = """
Japanese tokenization requires extra dependencies, but you do not have them installed.
Please install them like so.

    pip install sacrebleu[ja]
"""

class TokenizerJaMecab(NoneTokenizer):
    def __init__(self):
        if MeCab is None:
            raise RuntimeError(FAIL_MESSAGE)
        self.tagger = MeCab.Tagger(ipadic.MECAB_ARGS + " -Owakati")

        # make sure the dictionary is IPA
        d = self.tagger.dictionary_info()
        assert d.size == 392126, \
            "Please make sure to use the IPA dictionary for MeCab"
        # This asserts that no user dictionary has been loaded
        assert d.next is None

    def __call__(self, line):
        """
        Tokenizes an Japanese input line using MeCab morphological analyzer.

        :param line: a segment to tokenize
        :return: the tokenized line
        """
        line = line.strip()
        sentence = self.tagger.parse(line).strip()
        return sentence

    def signature(self):
        """
        Returns the MeCab parameters.

        :return: signature string
        """
        signature = self.tagger.version() + "-IPA"
        return 'ja-mecab-' + signature
