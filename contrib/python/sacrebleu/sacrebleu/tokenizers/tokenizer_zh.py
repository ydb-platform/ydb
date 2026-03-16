# -*- coding: utf-8 -*-

# Copyright 2017--2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not
# use this file except in compliance with the License. A copy of the License
# is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

##############

# MIT License
# Copyright (c) 2017 - Shujian Huang <huangsj@nju.edu.cn>

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# Author: Shujian Huang huangsj@nju.edu.cn


from .tokenizer_none import NoneTokenizer
from .tokenizer_re import TokenizerRegexp

_UCODE_RANGES = [
    (u'\u3400', u'\u4db5'),  # CJK Unified Ideographs Extension A, release 3.0
    (u'\u4e00', u'\u9fa5'),  # CJK Unified Ideographs, release 1.1
    (u'\u9fa6', u'\u9fbb'),  # CJK Unified Ideographs, release 4.1
    (u'\uf900', u'\ufa2d'),  # CJK Compatibility Ideographs, release 1.1
    (u'\ufa30', u'\ufa6a'),  # CJK Compatibility Ideographs, release 3.2
    (u'\ufa70', u'\ufad9'),  # CJK Compatibility Ideographs, release 4.1
    (u'\u20000', u'\u2a6d6'),  # (UTF16) CJK Unified Ideographs Extension B, release 3.1
    (u'\u2f800', u'\u2fa1d'),  # (UTF16) CJK Compatibility Supplement, release 3.1
    (u'\uff00', u'\uffef'),  # Full width ASCII, full width of English punctuation,
                             # half width Katakana, half wide half width kana, Korean alphabet
    (u'\u2e80', u'\u2eff'),  # CJK Radicals Supplement
    (u'\u3000', u'\u303f'),  # CJK punctuation mark
    (u'\u31c0', u'\u31ef'),  # CJK stroke
    (u'\u2f00', u'\u2fdf'),  # Kangxi Radicals
    (u'\u2ff0', u'\u2fff'),  # Chinese character structure
    (u'\u3100', u'\u312f'),  # Phonetic symbols
    (u'\u31a0', u'\u31bf'),  # Phonetic symbols (Taiwanese and Hakka expansion)
    (u'\ufe10', u'\ufe1f'),
    (u'\ufe30', u'\ufe4f'),
    (u'\u2600', u'\u26ff'),
    (u'\u2700', u'\u27bf'),
    (u'\u3200', u'\u32ff'),
    (u'\u3300', u'\u33ff'),
]


class TokenizerZh(NoneTokenizer):

    def signature(self):
        return 'zh'

    def __init__(self):
        self._post_tokenizer = TokenizerRegexp()

    @staticmethod
    def _is_chinese_char(uchar):
        """
        :param uchar: input char in unicode
        :return: whether the input char is a Chinese character.
        """
        for start, end in _UCODE_RANGES:
            if start <= uchar <= end:
                return True
        return False

    def __call__(self, line):
        """The tokenization of Chinese text in this script contains two
        steps: separate each Chinese characters (by utf-8 encoding); tokenize
        the non Chinese part (following the `13a` i.e. mteval tokenizer).

        Author: Shujian Huang huangsj@nju.edu.cn

        :param line: input sentence
        :return: tokenized sentence
        """

        line = line.strip()

        # TODO: the below code could probably be replaced with the following:
        # import regex
        # line = regex.sub(r'(\p{Han})', r' \1 ', line)
        line_in_chars = ""
        for char in line:
            if self._is_chinese_char(char):
                line_in_chars += " "
                line_in_chars += char
                line_in_chars += " "
            else:
                line_in_chars += char
        line = line_in_chars

        return self._post_tokenizer(line)
