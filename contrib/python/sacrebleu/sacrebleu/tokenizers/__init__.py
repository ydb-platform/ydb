# -*- coding: utf-8 -*-

from .tokenizer_none import NoneTokenizer
from .tokenizer_13a import Tokenizer13a
from .tokenizer_intl import TokenizerV14International
from .tokenizer_zh import TokenizerZh
from .tokenizer_ja_mecab import TokenizerJaMecab
from .tokenizer_char import TokenizerChar


DEFAULT_TOKENIZER = '13a'


TOKENIZERS = {
    'none': NoneTokenizer,
    '13a': Tokenizer13a,
    'intl': TokenizerV14International,
    'zh': TokenizerZh,
    'ja-mecab': TokenizerJaMecab,
    'char': TokenizerChar,
}
