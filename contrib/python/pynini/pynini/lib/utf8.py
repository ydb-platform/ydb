# Copyright 2016-2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# For general information on the Pynini grammar compilation library, see
# pynini.opengrm.org.
"""Rules to recognise RFC3629-compliant utf8 characters at the byte level."""

import pynini


def _byte_range(min_val, max_val=None) -> pynini.Fst:
  if max_val is None:
    max_val = min_val
  return pynini.union(*(f"[{i}]"
                        for i in range(min_val, max_val + 1))).optimize()

# Any character represented by a single byte (equivalent to ASCII).
# Not including ASCII NUL which would be kind of weird to match.
SINGLE_BYTE = _byte_range(0x01, 0x7F)

# Leading bytes for a 2-byte UTF8 character in RFC3629.
_leading_byte_2_byte = _byte_range(0xC2, 0xDF)

# Leading bytes for a 3-byte UTF8 character.
_leading_byte_3_byte_0xE0 = _byte_range(0xE0)
_leading_byte_3_byte_0xE1_EC = _byte_range(0xE1, 0xEC)
_leading_byte_3_byte_0xED = _byte_range(0xED)
_leading_byte_3_byte_0xEE_EF = _byte_range(0xEE, 0xEF)
_leading_byte_3_byte = pynini.union(
    _leading_byte_3_byte_0xE0,
    _leading_byte_3_byte_0xE1_EC,
    _leading_byte_3_byte_0xED,
    _leading_byte_3_byte_0xEE_EF,
).optimize()

# Leading bytes for a 4-byte UTF8 character.
_leading_byte_4_byte_0xF0 = _byte_range(0xF0)
_leading_byte_4_byte_0xF1_F3 = _byte_range(0xF1, 0xF3)
_leading_byte_4_byte_0xF4 = _byte_range(0xF4)
_leading_byte_4_byte = pynini.union(
    _leading_byte_4_byte_0xF0,
    _leading_byte_4_byte_0xF1_F3,
    _leading_byte_4_byte_0xF4,
).optimize()

# Leading bytes which indicate that the subsequent bytes form a single
# codepoint.
LEADING_BYTE = pynini.union(_leading_byte_2_byte, _leading_byte_3_byte,
                            _leading_byte_4_byte).optimize()

# Continued bytes.
_continue_byte_0xA0_BF = _byte_range(0xA0, 0xBF)
_continue_byte_0x80_9F = _byte_range(0x80, 0x9F)
_continue_byte_0x90_BF = _byte_range(0x90, 0xBF)
_continue_byte_0x80_8F = _byte_range(0x80, 0x8F)
CONTINUATION_BYTE = _byte_range(0x80, 0xBF)

# All valid constituent bytes of a valid utf-8 sequence.
VALID_BYTE = pynini.union(SINGLE_BYTE, LEADING_BYTE,
                          CONTINUATION_BYTE).optimize()

# A sequence of bytes making up a valid UTF8 character according to RFC3629
# See https://tools.ietf.org/html/rfc3629#section-4 for the reference ABNF.
VALID_UTF8_CHAR = pynini.union(
    # 1-byte sequences
    SINGLE_BYTE,
    # 2-byte sequences
    _leading_byte_2_byte + CONTINUATION_BYTE,
    # 3-byte sequences
    _leading_byte_3_byte_0xE0 + _continue_byte_0xA0_BF + CONTINUATION_BYTE,
    _leading_byte_3_byte_0xE1_EC + CONTINUATION_BYTE ** 2,
    _leading_byte_3_byte_0xED + _continue_byte_0x80_9F + CONTINUATION_BYTE,
    _leading_byte_3_byte_0xEE_EF + CONTINUATION_BYTE ** 2,
    # 4-byte sequences
    _leading_byte_4_byte_0xF0 + _continue_byte_0x90_BF + CONTINUATION_BYTE ** 2,
    _leading_byte_4_byte_0xF1_F3 + CONTINUATION_BYTE ** 3,
    _leading_byte_4_byte_0xF4 + _continue_byte_0x80_8F + CONTINUATION_BYTE ** 2,
).optimize()

# A sequence of bytes making up a regional indicator symbol, each of which
# represent a letter. We need to call these out explicitly for classification
# since pairs of these symbols represent national flags, e.g. the flag of France
# is 'F' & 'R'. (https://en.wikipedia.org/wiki/Regional_Indicator_Symbol)
VALID_UTF8_CHAR_REGIONAL_INDICATOR_SYMBOL = (
    "[240][159][135]" +
    pynini.union(*(f"[{i}]" for i in range(166, 192)))).optimize()

