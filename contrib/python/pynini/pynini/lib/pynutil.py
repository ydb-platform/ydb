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
"""Simple utility functions.

Please do not add overly-specific functions or introduce additional
dependencies (i.e., beyond Pynini itself) to this module.
"""

from typing import Optional

import pynini


def add_weight(expr: pynini.FstLike,
               weight: pynini.WeightLike) -> pynini.Fst:
  """Attaches a weight to an automaton.

  Args:
    expr: an automaton or string.
    weight: a weight or string.

  Returns:
    An FST.
  """
  return pynini.accep("", weight=weight).concat(expr)


def insert(expr: pynini.FstLike,
           weight: Optional[pynini.WeightLike] = None) -> pynini.Fst:
  """Creates the transducer for <epsilon> x expr.

  Args:
    expr: an acceptor or string.
    weight: an optional weight or string.

  Returns:
    An FST.
  """
  result = pynini.cross("", expr)
  if weight is not None:
    return add_weight(result, weight)
  else:
    return result


def delete(expr: pynini.FstLike,
           weight: Optional[pynini.WeightLike] = None) -> pynini.Fst:
  """Creates the transducer for expr x <epsilon>.

  Args:
    expr: an acceptor or string.
    weight: an optional weight or string.

  Returns:
    An FST.
  """
  result = pynini.cross(expr, "")
  if weight is not None:
    return add_weight(result, weight)
  else:
    return result


def join(expr: pynini.FstLike, sep: pynini.FstLike) -> pynini.Fst:
  """Creates the automaton expr (sep expr)^*.

  Args:
    expr: an automaton or string.
    sep: a separator acceptor or string.

  Returns:
    An FST.
  """
  cdr = pynini.concat(sep, expr).closure()
  return expr + cdr

