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
r"""Tagger automaton class.

This module defines a Taggger class which uses an FST to tag all instances of
a set of strings in a text. The user provides a tag label,
a (possibly weighted) acceptor matching the strings to be tagged,
and unweighted cyclic acceptor \Sigma^*.

At inference time, we compose the string with the rewrite rule transducer,
compute the shortest-path, and take the output string. The shortest-path
step is necessary as there is no guarantee that the rewrite rule transducer
is functional (for example, one string in the "vocabulary" could contain
another).
"""

import pynini
from pynini.lib import pynutil
from pynini.lib import rewrite


class Error(Exception):

  pass


class Tagger:
  """FST-based tagger."""

  LTAG_TEMPLATE = "<{}>"
  RTAG_TEMPLATE = "</{}>"

  def __init__(self, tag_label: str, matcher: pynini.FstLike,
               sigma_star: pynini.FstLike) -> None:
    """Constructor.

    Args:
      tag_label: String used as a tag. It must be in-alphabet when processed by
        the specified token type.
      matcher: an acceptor matching the strings to be tagged.
      sigma_star: an unweighted cyclic acceptor over the vocabulary.

    Raises:
        Error: Tag is not in the alphabet.
    """
    # Builds tag transducer.
    ltag = pynutil.insert(self.LTAG_TEMPLATE.format(tag_label))
    rtag = pynutil.insert(self.RTAG_TEMPLATE.format(tag_label))
    self._tagger = pynini.cdrewrite(ltag + matcher + rtag, "", "",
                                    sigma_star).optimize()

  def tag(self, string: pynini.FstLike) -> str:
    """Tags an input string.

    This method inserts XML-style tags around all substrings in the input
    string matching any element in the vocabulary.

    Args:
      string: The input string.

    Returns:
      The tagged string.
    """
    return rewrite.one_top_rewrite(string, self._tagger)

