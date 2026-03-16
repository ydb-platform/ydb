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
"""Sketch of Finnish case suffix rules.

This is loosely based on the "archiphonemic" treatment of harmony used by:

Koskenniemi, K. 1983. Two-Level Morphology: A General Computational Model for
Word-Form Recognition and Production. Doctoral dissertation, University of
Helsinki.

Seven of the nine locative cases are covered; the others pose additional
(archiphonemic) challenges.

* abessive ("without"): -tta, -ttä
* ablative ("from at/on"): -lta, -ltä
* adessive ("on"): -lla, -llä
* allative ("onto"): -lle
* elative ("from in"): -sta, -stä
* essive ("as a"): -na, -nä
* inessive ("in"): -ssa, -ssä

Gradation is not handled in any form.
"""

import pynini
from pynini.lib import rewrite

# Inventories.
_back = pynini.union("a", "o", "u")
_front = pynini.union("ä", "ö", "y")
_neutral = pynini.union("e", "i")
_v = pynini.union(_back, _front, _neutral, "A")
_c = pynini.union("b", "c", "d", "f", "g", "h", "j", "k", "l", "m", "n", "p",
                  "q", "r", "s", "t", "v", "w", "x", "z")
_sigma_star = pynini.union(_v, _c).closure().optimize()

# Rules.
_harmony = (pynini.cdrewrite(
    pynini.cross("A", "a"), _back + pynini.union(_neutral, _c).closure(), "",
    _sigma_star) @ pynini.cdrewrite(
        pynini.cross("A", "ä"), "", "", _sigma_star)).optimize()


# Functions.


def _harmonic_suffix(stem: str, suffix: str) -> str:
  """Concatenates suffix and applies the harmony rule."""
  return rewrite.one_top_rewrite(stem + suffix, _harmony)


def abessive(stem: str) -> str:
  return _harmonic_suffix(stem, "ttA")


def ablative(stem: str) -> str:
  return _harmonic_suffix(stem, "ltA")


def adessive(stem: str) -> str:
  return _harmonic_suffix(stem, "llA")


def allative(stem: str) -> str:
  # There's no harmonic suffix vowel here, but there's no harm using it.
  return _harmonic_suffix(stem, "lle")


def elative(stem: str) -> str:
  return _harmonic_suffix(stem, "stA")


def essive(stem: str) -> str:
  return _harmonic_suffix(stem, "nA")


def inessive(stem: str) -> str:
  return _harmonic_suffix(stem, "ssA")

