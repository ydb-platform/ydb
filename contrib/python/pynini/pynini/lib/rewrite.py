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
"""Rewriter functions for Pynini.

This module provides functions for rewriting strings using WFST rules. While
this may seem like a relatively easy problem, there are a number of potential
pitfalls, which these implementations address. For specific examples, see the
unit test.

There are five different end-user functions for rewriting:

* `matches` tests whether a given input/output pair is compatible
  with a rule.
* `rewrites` returns a list of all valid rewrites.
* `top_rewrites` returns a list of the n top rewrites.
* `top_rewrite` returns the shortest-path rewrite; in case of ties, which
  string is returned is implementation-defined.
* `one_top_rewrite` returns the shortest-path rewrite; in case of ties, an
  exception is raised.
* `optimal_rewrites` returns a list of all rewrites whose weight is the same
  as the shortest-path rewrite.

The following helper functions are also exposed:

* `rewrite_lattice` creates an epsilon-free lattice of output strings
  from an input FST or string and a rule FST.
* `lattice_to_dfa` converts that lattice to a DFA, pruning at semiring one
  if `optimal=True`.
* `lattice_to_nshortest` creates a lattice of the n-shortest strings.
* `lattice_to_top_string` extracts a single string from a lattice; in case of
  ties, which string is returned is implementation-defined.
* `lattice_to_one_top_string` extracts a single string from a pruned DFA,
  raising an error if there is more than one.
* `lattice_to_strings` returns a list of all output strings in a lattice.
"""

from typing import List, Optional

import logging

import pynini


class Error(Exception):
  """Errors specific to this module."""

  pass


# Helper functions.


def rewrite_lattice(
    string: pynini.FstLike,
    rule: pynini.Fst,
    token_type: Optional[pynini.TokenType] = None) -> pynini.Fst:
  """Constructs a weighted lattice of output strings.

  Constructs a weighted, epsilon-free lattice of output strings given an
  input FST (or string) and a rule FST.

  Args:
    string: Input string or FST.
    rule: Input rule WFST.
    token_type: Optional input token type, or symbol table.

  Returns:
    An epsilon-free WFSA.

  Raises:
    Error: Composition failure.
  """
  # TODO(kbg): Consider adding support for PDT and MPDT composition.
  # TODO(kbg): Consider using `contextlib.nullcontext` here instead.
  if token_type is None:
    lattice = pynini.compose(string, rule, compose_filter="alt_sequence")
  else:
    with pynini.default_token_type(token_type):
      lattice = pynini.compose(string, rule, compose_filter="alt_sequence")
  if lattice.start() == pynini.NO_STATE_ID:
    raise Error("Composition failure")
  return lattice.project("output").rmepsilon()


def lattice_to_dfa(lattice: pynini.Fst,
                   optimal_only: bool,
                   state_multiplier: int = 4) -> pynini.Fst:
  """Constructs a (possibly pruned) weighted DFA of output strings.

  Given an epsilon-free lattice of output strings (such as produced by
  rewrite_lattice), attempts to determinize it, pruning non-optimal paths if
  optimal_only is true. This is valid only in a semiring with the path property.

  To prevent unexpected blowup during determinization, a state threshold is
  also used and a warning is logged if this exact threshold is reached. The
  threshold is a multiplier of the size of input lattice (by default, 4), plus
  a small constant factor. This is intended by a sensible default and is not an
  inherently meaningful value in and of itself.

  Args:
    lattice: Epsilon-free non-deterministic finite acceptor.
    optimal_only: Should we only preserve optimal paths?
    state_multiplier: Max ratio for the number of states in the DFA lattice to
      the NFA lattice; if exceeded, a warning is logged.

  Returns:
    Epsilon-free deterministic finite acceptor.
  """
  weight_type = lattice.weight_type()
  weight_threshold = (
      pynini.Weight.one(weight_type)
      if optimal_only else pynini.Weight.zero(weight_type))
  state_threshold = 256 + state_multiplier * lattice.num_states()
  lattice = pynini.determinize(
      lattice, nstate=state_threshold, weight=weight_threshold)
  if lattice.num_states() == state_threshold:
    logging.warning("Unexpected hit state threshold; consider a higher value "
                    "for state_multiplier")
  return lattice


def lattice_to_nshortest(lattice: pynini.Fst, nshortest: int) -> pynini.Fst:
  """Returns the n-shortest unique paths as an FST.

  Given an epsilon-free lattice of output strings (such as produced by
  rewrite_lattice), extracts the n-shortest unique strings. This is valid only
  in a path semiring.

  Args:
    lattice: Epsilon-free finite acceptor.
    nshortest: Maximum number of shortest paths desired.

  Returns:
    A lattice of the n-shortest unique paths.
  """
  return pynini.shortestpath(lattice, nshortest=nshortest, unique=True)


def lattice_to_top_string(lattice: pynini.Fst,
                          token_type: Optional[pynini.TokenType] = None) -> str:
  """Returns the top string in the lattice.

  Given an epsilon-free lattice of output strings (such as produced by
  rewrite_lattice), extracts a single top string. This is valid only in a path
  semiring.

  Args:
    lattice: Epsilon-free finite acceptor.
    token_type: Optional output token type, or symbol table.

  Returns:
    The top string.
  """
  return pynini.shortestpath(lattice).string(token_type)


def lattice_to_strings(
    lattice: pynini.Fst,
    token_type: Optional[pynini.TokenType] = None) -> List[str]:
  """Returns tuple of output strings.

  Args:
    lattice: Epsilon-free acyclic WFSA.
    token_type: Optional output token type, or symbol table.

  Returns:
    An list of output strings.
  """
  return list(lattice.paths(output_token_type=token_type).ostrings())


def lattice_to_one_top_string(lattice: pynini.Fst,
                              token_type: Optional[pynini.TokenType] = None
                             ) -> str:
  """Returns the top string in the lattice, raising an error if there's a tie.

  Given a pruned DFA of output strings (such as produced by lattice_to_dfa
  with optimal_only), extracts a single top string, raising an error if there's
  a tie.

  Args:
    lattice: Epsilon-free deterministic finite acceptor.
    token_type: Optional output token type, or symbol table.

  Returns:
    The top string.

  Raises:
    Error: Multiple top rewrites found.
  """
  spaths = lattice.paths(output_token_type=token_type)
  output = spaths.ostring()
  spaths.next()
  if not spaths.done():
    raise Error(
        "Multiple top rewrites found: "
        f"{output!r} and {spaths.ostring()!r} (weight: {spaths.weight()})")
  return output


# Rewrite functions.


def matches(istring: pynini.FstLike,
            ostring: pynini.FstLike,
            rule: pynini.Fst,
            input_token_type: Optional[pynini.TokenType] = None,
            output_token_type: Optional[pynini.TokenType] = None) -> bool:
  """Returns whether an input-output pair is generated by a rule.

  Args:
    istring: Input string or FST.
    ostring: Output string or FST.
    rule: Rule FST.
    input_token_type: Optional input token type, or symbol table.
    output_token_type: Optional output token type, or symbol table.

  Returns:
    Whether the input-output pair is generated by the rule.
  """
  lattice = rewrite_lattice(istring, rule, input_token_type)
  # TODO(kbg): Consider using `contextlib.nullcontext` here instead.
  if output_token_type is None:
    lattice = pynini.intersect(lattice, ostring, compose_filter="sequence")
  else:
    with pynini.default_token_type(output_token_type):
      lattice = pynini.intersect(lattice, ostring, compose_filter="sequence")
  return lattice.start() != pynini.NO_STATE_ID


def rewrites(string: pynini.FstLike,
             rule: pynini.Fst,
             input_token_type: Optional[pynini.TokenType] = None,
             output_token_type: Optional[pynini.TokenType] = None,
             state_multiplier: int = 4) -> List[str]:
  """Returns all rewrites.

  Args:
    string: Input string or FST.
    rule: Input rule WFST.
    input_token_type: Optional input token type, or symbol table.
    output_token_type: Optional output token type, or symbol table.
    state_multiplier: Max ratio for the number of states in the DFA lattice to
      the NFA lattice; if exceeded, a warning is logged.

  Returns:
    A list of output strings.
  """
  lattice = rewrite_lattice(string, rule, input_token_type)
  lattice = lattice_to_dfa(lattice, False, state_multiplier)
  return lattice_to_strings(lattice, output_token_type)


def top_rewrites(
    string: pynini.FstLike,
    rule: pynini.Fst,
    nshortest: int,
    input_token_type: Optional[pynini.TokenType] = None,
    output_token_type: Optional[pynini.TokenType] = None) -> List[str]:
  """Returns the top n rewrites.

  Args:
    string: Input string or FST.
    rule: Input rule WFST.
    nshortest: The maximum number of rewrites to return.
    input_token_type: Optional input token type, or symbol table.
    output_token_type: Optional output token type, or symbol table.

  Returns:
    A list of output strings.
  """
  lattice = rewrite_lattice(string, rule, input_token_type)
  lattice = lattice_to_nshortest(lattice, nshortest)
  return lattice_to_strings(lattice, output_token_type)


def top_rewrite(string: str,
                rule: pynini.Fst,
                input_token_type: Optional[pynini.TokenType] = None,
                output_token_type: Optional[pynini.TokenType] = None) -> str:
  """Returns one top rewrite.

  Args:
    string: Input string or FST.
    rule: Input rule WFST.
    input_token_type: Optional input token type, or symbol table.
    output_token_type: Optional output token type, or symbol table.

  Returns:
    The top string.
  """
  lattice = rewrite_lattice(string, rule, input_token_type)
  return lattice_to_top_string(lattice, output_token_type)


def one_top_rewrite(string: str,
                    rule: pynini.Fst,
                    input_token_type: Optional[pynini.TokenType] = None,
                    output_token_type: Optional[pynini.TokenType] = None,
                    state_multiplier: int = 4) -> str:
  """Returns one top rewrite, unless there is a tie.

  Args:
    string: Input string or FST.
    rule: Input rule WFST.
    input_token_type: Optional input token type, or symbol table.
    output_token_type: Optional output token type, or symbol table.
    state_multiplier: Max ratio for the number of states in the DFA lattice to
      the NFA lattice; if exceeded, a warning is logged.

  Returns:
    The top string.
  """
  lattice = rewrite_lattice(string, rule, input_token_type)
  lattice = lattice_to_dfa(lattice, True, state_multiplier)
  return lattice_to_one_top_string(lattice, output_token_type)


def optimal_rewrites(string: pynini.FstLike,
                     rule: pynini.Fst,
                     input_token_type: Optional[pynini.TokenType] = None,
                     output_token_type: Optional[pynini.TokenType] = None,
                     state_multiplier: int = 4) -> List[str]:
  """Returns all optimal rewrites.

  Args:
    string: Input string or FST.
    rule: Input rule WFST.
    input_token_type: Optional input token type, or symbol table.
    output_token_type: Optional output token type, or symbol table.
    state_multiplier: Max ratio for the number of states in the DFA lattice to
      the NFA lattice; if exceeded, a warning is logged.

  Returns:
    A tuple of output strings.
  """
  lattice = rewrite_lattice(string, rule, input_token_type)
  lattice = lattice_to_dfa(lattice, True, state_multiplier)
  return lattice_to_strings(lattice, output_token_type)

