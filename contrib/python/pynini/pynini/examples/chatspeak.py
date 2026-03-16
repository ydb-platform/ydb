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
"""Grammar to handle the "channel" part of source-channel chatspeak normalizer.

We consider the following cases

1) Common chatspeak terms, from a lexicon.
2) Common patterns, as described by regular expression.
3) Letter-duplication cases, such as "cooooooool"
4) Some abbreviations with optional sonorant and vowel deletions.

Chatspeak terms are normalized by this grammar to a lattice of possible
verbalizations for a sentence, which then gets scored by a language model.

For simplicity we assume that all text is case-free (lower case).
"""

import string

import pynini
from pynini.lib import byte
from pynini.lib import pynutil
from pynini.lib import rewrite

# Helpers.


def _plus(token: pynini.FstLike) -> pynini.Fst:
  return pynini.closure(token, 1)


def _star(token: pynini.FstLike) -> pynini.Fst:
  return pynini.closure(token)


def _ques(token: pynini.FstLike) -> pynini.Fst:
  return pynini.closure(token, 0, 1)


# The alphabet.

_sigma_star = pynini.closure(byte.BYTE).optimize()

# The four normalization engines:
#
# * deduplicator
# * regular expressions
# * abbreviation expander
# * a lexicon


class Deduplicator:
  """Container for a deduplicator for all letters."""

  _dedup: pynini.Fst
  _lexicon: pynini.Fst

  def __init__(self, lexicon: pynini.Fst):
    """Constructs the deduplicator.

    Args:
      lexicon: an FSA representing the lexicon.
    """
    it = iter(string.ascii_lowercase)
    letter = next(it)
    self._dedup = Deduplicator.dedup_rule(letter)
    for letter in it:
      self._dedup @= Deduplicator.dedup_rule(letter)
      self._dedup.optimize()
    self._lexicon = lexicon

  @staticmethod
  def dedup_rule(letter: str) -> pynini.Fst:
    """Compiles transducer that optionally deletes multiple letters.

    One or two of the same letter must be encountered beforehand.

    Args:
      letter: a letter.

    Returns:
      An FST deleting that in an appropriate sequence.
    """
    not_letter = byte.LOWER - letter
    return pynini.cdrewrite(
        pynini.cross(_plus(letter), _ques(letter)),
        ("[BOS]" | not_letter) + letter, ("[EOS]" | not_letter), _sigma_star)

  def expand(self, token: pynini.FstLike) -> pynini.Fst:
    """Finds deduplication candidates for a token in a lexicon.

    Args:
      token: a "cooooool"-like token.

    Returns:
      An FST representing a lattice of possible matches.
    """
    try:
      lattice = rewrite.rewrite_lattice(token, self._dedup)
      return rewrite.rewrite_lattice(lattice, self._lexicon)
    except rewrite.Error:
      return pynini.Fst()


class Deabbreviator:
  """Expands abbreviations formed by deleting vowels or sonorants.

  The result must have at least two letters and should not already be in the
  lexicon (i.e., we don't wish to expand things that could be words), and the
  expansion should have at least three letters.
  """

  _v = pynini.union("a", "e", "i", "o", "u")
  _r = pynini.union("r", "l", "n")
  _c = byte.LOWER - _v
  # We must delete a whole vowel span, which may include "y" at the end.
  _vowel_span = _plus(_v) + _ques("y")
  _r_deletion = pynini.cdrewrite(
      pynutil.delete(_r), _v, _c - _r, _sigma_star, mode="opt").optimize()
  _v_deletion = pynini.cdrewrite(
      pynutil.delete(_vowel_span), _c, _c | "[EOS]", _sigma_star,
      mode="opt").optimize()
  _three_letters = byte.LOWER**(3, ...)

  _deabbrev: pynini.Fst

  def __init__(self, lexicon: pynini.Fst):
    """Constructs the deabbreviator.

    Args:
      lexicon: an FSA representing the lexicon.
    """
    two_letters_not_in_lexicon = byte.LOWER**(2, ...) - lexicon
    two_letters_not_in_lexicon.optimize()
    rules = lexicon @ self._three_letters @ self._r_deletion @ self._v_deletion
    self._deabbrev = two_letters_not_in_lexicon @ rules.invert()
    self._deabbrev.optimize()

  def expand(self, token: pynini.FstLike) -> pynini.Fst:
    try:
      return rewrite.rewrite_lattice(token, self._deabbrev)
    except rewrite.Error:
      return pynini.Fst()


class Regexps:
  """Container for regexp substitutions."""

  # TODO(rws): The following regexps are based on an internal, now defunct, #
  #  chatspeak project. I need to make sure that it is OK to release these.
  _regexps = pynini.union(
      pynini.cross("b" + _plus("b") + _star("z"), "bye bye"),
      pynini.cross("congrat" + _star("z"), "congratulations"),
      pynini.cross("cool" + _plus("z"), "cool"),
      pynini.cross("delis" + _plus("h"), "delicious"),
      pynini.cross("e" + _plus("r"), "uh"),  # Forcing newline.
      pynini.cross("f" + _plus("f"), "ff"),
      pynini.cross("g" + _plus("l"), "good luck"),
      pynini.cross("he" + _plus("y") + "z", "hey"),
      pynini.cross("he" + _ques("'") + _plus("z"), "he's"),
      pynini.cross("how" + _ques("'") + _plus("z"), "how is"),
      pynini.cross("how" + _ques("'") + _plus("z"), "how has"),
      pynini.cross("how" + _ques("'") + _plus("z"), "how was"),
      pynini.cross("how" + _ques("'") + _plus("z"), "how does"),
      pynini.cross("kew" + _plus("l") + _star("z"), "cool"),
      pynini.cross("k" + _plus("k"), "ok"),
      pynini.cross("ko" + _plus("o") + "l", "cool"),
      pynini.cross("k" + _plus("z"), "ok"),
      pynini.cross(
          _plus("l") + pynini.union("o", "u").closure(1) + _plus("l") +
          _plus("z"), pynini.union("laugh out loud", "laugh")),
      pynini.cross(_plus("l") + _plus("u") + _plus("r") + _plus("v"), "love"),
      pynini.cross(
          _plus("l") + _plus("u") + _plus("v") + _plus("e") + _plus("e"),
          "love"),
      pynini.cross("mis" + _plus("h"), "miss"),
      pynini.cross("m" + _plus("m") + "k", "mm ok"),
      pynini.cross("n00b" + _plus("z"), "newbie"),
      pynini.cross("na" + _plus("h"), "no"),
      pynini.cross("no" + _plus("e") + _plus("z"), "no"),
      pynini.cross("noob" + _plus("z"), "newbie"),
      pynini.cross("oke" + _plus("e"), "okay"),
      pynini.cross("oki" + _plus("e"), "okay"),
      pynini.cross("ok" + _plus("z"), "okay"),
      pynini.cross("om" + _plus("g"), "oh my god"),
      pynini.cross("omg" + _plus("z"), "oh my god"),
      pynini.cross("orly" + _plus("z"), "oh really"),
      pynini.cross("pl" + _plus("z"), "please"),
      pynini.cross("pw" + _plus("e") + "ase", "please"),
      pynini.cross("q" + _plus("n"), "_question"),
      pynini.cross("qool" + _plus("z"), "cool"),
      pynini.cross("rox0r" + _plus("z"), "rocks"),
      pynini.cross("sorry" + _plus("z"), "sorry"),
      pynini.cross("s" + _plus("o") + "w" + _plus("w") + _plus("y"), "sorry"),
      pynini.cross("sry" + _plus("z"), "sorry"),
      pynini.cross("thanke" + _plus("w"), "thank you"),
      pynini.cross("thank" + _plus("q"), "thank you"),
      pynini.cross("t" + _plus("q"), "thank you"),
      pynini.cross("t" + _plus("y"), "thank you"),
      pynini.cross("tyv" + _plus("m"), "thank you very much"),
      pynini.cross(_plus("u"), "you"),  # Forcing newline.
      pynini.cross("ug" + _plus("h"), "ugh"),
      pynini.cross("u" + _plus("h"), "uh"),
      pynini.cross("wai" + _plus("t"), "wait"),
      pynini.cross("w" + _plus("a") + _plus("z"), "what's"),
      pynini.cross("w" + _plus("a") + _plus("z") + _plus("a"), "what's up"),
      pynini.cross(
          _plus("wa") + _plus("z") + _plus("u") + _plus("p"), "what's up"),
      pynini.cross("wh" + _plus("a"), "what"),
      pynini.cross("w" + _plus("u") + _plus("t"), "what"),
      pynini.cross(_plus("xo"), "'hugs and kisses'"),
      pynini.cross("ya" + _plus("h"), "yeah"),
      pynini.cross("ya" + _plus("r"), "yeah"),
      pynini.cross("ye" + _plus("a"), "yeah"),
      pynini.cross("yes" + _plus("h"), "yes"),
      pynini.cross("ye" + _plus("z"), "yes"),
      pynini.cross("yup" + _plus("s"), "yup"),
      pynini.cross("yup" + _plus("z"), "yup"),
      pynini.cross("zom" + _plus("g"), "oh my god"),
      pynini.cross("z" + _plus("u") + _plus("p"), "what's up")).optimize()

  def expand(self, token: pynini.FstLike) -> pynini.Fst:
    """Finds regexps candidates for a token.

    Args:
      token: a "zomggg"-like token.

    Returns:
      An FST representing a lattice of possible matches.
    """
    try:
      return rewrite.rewrite_lattice(token, self._regexps)
    except rewrite.Error:
      return pynini.Fst()


class Lexicon:
  """Container for a substitution lexicon."""

  _lexicon: pynini.Fst

  def __init__(self, path: str):
    self._lexicon = pynini.string_file(path).optimize()

  def expand(self, token: pynini.FstLike) -> pynini.Fst:
    try:
      return rewrite.rewrite_lattice(token, self._lexicon)
    except rewrite.Error:
      return pynini.Fst()

