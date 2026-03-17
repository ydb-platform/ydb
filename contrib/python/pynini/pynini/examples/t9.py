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
"""Simple implementation of a T9 processor.

For a given T9 input it returns a lattice of decoded phrases consistent with
the provided lexicon. The user can choose to rescore (e.g., with a language
model) this lattice if desired.
"""

from typing import Iterable

import pynini
from pynini.lib import pynutil
from pynini.lib import rewrite


class T9:
  """Simple implementation of a T9 processor."""

  _t9_map = [("0", [" "]), ("2", ["a", "b", "c"]), ("3", ["d", "e", "f"]),
             ("4", ["g", "h", "i"]), ("5", ["j", "k", "l"]),
             ("6", ["m", "n", "o"]), ("7", ["p", "q", "r", "s"]),
             ("8", ["t", "u", "v"]), ("9", ["w", "x", "y", "z"])]

  def __init__(self, lexicon: Iterable[str]):
    self._make_fst()
    self._make_lexicon(lexicon)

  def _make_fst(self) -> None:
    self._decoder = pynini.Fst()
    for (inp, outs) in self._t9_map:
      self._decoder |= pynini.cross(inp, pynini.union(*outs))
    self._decoder.closure().optimize()
    self._encoder = pynini.invert(self._decoder)

  def _make_lexicon(self, lexicon: Iterable[str]) -> None:
    lexicon_fst = pynini.string_map(lexicon)
    self._lexicon = pynutil.join(lexicon_fst, " ").optimize()

  def decode(self, t9_input: pynini.FstLike) -> pynini.Fst:
    lattice = rewrite.rewrite_lattice(t9_input, self._decoder)
    return pynini.intersect(lattice, self._lexicon)

  def encode(self, text: pynini.FstLike) -> str:
    return rewrite.top_rewrite(text, self._encoder)

