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
"""Simple number name grammar for (American) English, to 10 million (exclusive).

This is loosely based on approach used by:

Sproat, R. 1996. Multilingual text analysis for text-to-speech synthesis.
Natural Language Engineering, 2(4): 369-380
"""

import string

import pynini
from pynini.lib import pynutil
from pynini.lib import rewrite

# Inventories.

_digit = pynini.union(*string.digits)
# Powers of ten that have single-word representations in English. E1*
# is a special symbol we will use in the teens below.
_powers = pynini.union("[E1]", "[E1*]", "[E2]", "[E3]", "[E6]")
_sigma_star = pynini.union(_digit, _powers).closure().optimize()

# Inserts factors in the appropriate place in the digit sequence.
_raw_factorizer = (
    _digit + pynutil.insert("[E6]") + _digit + pynutil.insert("[E2]") + _digit +
    pynutil.insert("[E1]") + _digit + pynutil.insert("[E3]") + _digit +
    pynutil.insert("[E2]") + _digit + pynutil.insert("[E1]") + _digit)

# Deletes "0" and "0" followed by a factor, so as to clear out unverbalized
# material in cases like "2,000,324". This needs to be done with some care since
# we need to keep E3 if it has a multiplier, but not if there is nothing between
# the thousands and the millions place.
_del_zeros = (
    pynini.cdrewrite(pynutil.delete("0"), "", "[EOS]", _sigma_star)
    @ pynini.cdrewrite(pynutil.delete("0[E1]"), "", "", _sigma_star)
    @ pynini.cdrewrite(pynutil.delete("0[E2]"), "", "", _sigma_star)
    @ pynini.cdrewrite(pynutil.delete("0[E3]"), "[E6]", "", _sigma_star)
    @ pynini.cdrewrite(pynutil.delete("0[E6]"), "", "", _sigma_star)
    @ pynini.cdrewrite(pynutil.delete("0"), "", "", _sigma_star)
).optimize()
# Inserts an arbitrary number of zeros at the beginning of a string so that
# shorter strings can match the length expected by the raw factorizer.
_pad_zeros = pynutil.insert("0").closure().concat(pynini.closure(_digit))

# Changes E1 to E1* for 11-19.
_fix_teens = pynini.cdrewrite(
    pynini.cross("[E1]", "[E1*]"), "1", _digit, _sigma_star)

# The actual factorizer
_phi = (_pad_zeros @ _raw_factorizer @ _del_zeros @ _fix_teens).optimize()

_lambda = pynini.string_map([("1", "one"), ("2", "two"), ("3", "three"),
                             ("4", "four"), ("5", "five"), ("6", "six"),
                             ("7", "seven"), ("8", "eight"), ("9", "nine"),
                             ("1[E1]", "ten"), ("1[E1*]1", "eleven"),
                             ("1[E1*]2", "twelve"), ("1[E1*]3", "thirteen"),
                             ("1[E1*]4", "fourteen"), ("1[E1*]5", "fifteen"),
                             ("1[E1*]6", "sixteen"), ("1[E1*]7", "seventeen"),
                             ("1[E1*]8", "eighteen"), ("1[E1*]9", "nineteen"),
                             ("2[E1]", "twenty"), ("3[E1]", "thirty"),
                             ("4[E1]", "forty"), ("5[E1]", "fifty"),
                             ("6[E1]", "sixty"), ("7[E1]", "seventy"),
                             ("8[E1]", "eighty"), ("9[E1]", "ninety"),
                             ("[E2]", "hundred"), ("[E3]", "thousand"),
                             ("[E6]", "million")]).optimize()
_lambda_star = pynutil.join(_lambda, pynutil.insert(" ")).optimize()


def number(token: str) -> str:
  return rewrite.one_top_rewrite(token, _phi @ _lambda_star)


# TODO(kbg): Remove this once weather.py no longer requires it.
# I would like to leave the "verbalized weatehr report" as an exercise to
# the reader.
VERBALIZE = _phi @ _lambda_star

