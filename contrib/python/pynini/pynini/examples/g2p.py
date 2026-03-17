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
"""Sketch of Spanish grapheme-to-phoneme conversion.

The dialect transcribed is roughly standard Mexican Spanish.
"""

import pynini
from pynini.lib import pynutil
from pynini.lib import rewrite

# Inventories.
_g = pynini.union("a", "á", "b", "c", "d", "e", "é", "f", "g", "h", "i", "í",
                  "j", "k", "l", "m", "n", "ñ", "o", "ó", "p", "q", "r", "s",
                  "t", "u", "ú", "ü", "v", "w", "x", "y", "z")
_p = pynini.union("a", "b", "d", "e", "f", "g", "i", "j", "k", "l", "ʝ", "m",
                  "n", "ɲ", "o", "p", "r", "ɾ", "s", "ʃ", "t", "u", "w", "x",
                  "z")
_sigma_star = pynini.union(_g, _p).closure().optimize()

# Rules.
_r1 = pynini.cdrewrite(
    pynini.string_map([
        ("ch", "tʃ"),
        ("ll", "ʝ"),
        ("qu", "k"),
        ("j", "x"),
        ("ñ", "ɲ"),
        ("v", "b"),
        ("x", "s"),
        ("y", "j"),
        ("á", "a"),
        ("é", "e"),
        ("í", "i"),
        ("ó", "o"),
        ("ú", "u"),
        ("ü", "w"),
    ]),
    "",
    "",
    _sigma_star,
).optimize()
_r2 = pynini.cdrewrite(pynutil.delete("h"), "", "", _sigma_star).optimize()
_v = pynini.union("a", "e", "i", "o", "u")
_r3 = pynini.cdrewrite(pynini.cross("r", "ɾ"), _v, _v, _sigma_star).optimize()
_r4 = pynini.cdrewrite(pynini.cross("rr", "r"), "", "", _sigma_star).optimize()
_r5 = pynini.cdrewrite(
    pynini.string_map([("c", "s"), ("g", "x")]), "", pynini.union("i", "e"),
    _sigma_star).optimize()
_r6 = pynini.cdrewrite(pynini.cross("c", "k"), "", "", _sigma_star).optimize()
_rules = _r1 @ _r2 @ _r3 @ _r4 @ _r5 @ _r6
_g2p = pynini.closure(_g) @ _rules @ pynini.closure(_p)
_g2p.optimize()

# Functions.


def g2p(string: str) -> str:
  return rewrite.one_top_rewrite(string, _g2p)

