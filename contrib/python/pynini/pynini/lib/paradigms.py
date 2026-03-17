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
r"""Implementation of paradigms, mirroring and extending functionality in Thrax.

This follows the treatment of morphology in chapter 2 of:

Roark, B. and Sproat, R. 2007. Computational Approaches to Morphology and
Syntax. Oxford University Press.

Following that work, all affixation is treated via composition, since that is
the most general single regular operation that can handle morphological
processes. For example, suffixing an "s" to a word, with a morpheme boundary "+"
is modeled as the following "shape":

\Sigma^* ({\epsilon} \times {+s})

i.e. an insertion of "+s" after anything, where \Sigma^* represents a string
of any symbol except the boundary symbol. If one wanted to also constrain the
stem to be of a particular shape (e.g., English -er comparatives, where a stem
must be at most two syllables long) or even to modify the stem to be a
particular shape (as in Yowlumne) then one can replace \Sigma^* with a more
restrictive acceptor or transducer.

A paradigm consists of:

- A Category.
- A list of tuples of a FeatureVector from the Category and a "shape" as above.
- A specification of what FeatureVector corresponds to the "lemma". For example
  in Latin nouns it would be "[case=nom][num=sg]". In Latin verbs it is the
  first person singular indicative active.
- A list of stem strings or acceptors.
- An optional name.
- An optional set of rewrite rules to apply to forms in the paradigm.
- An optional boundary symbol (defaults to "+").
- An optional parent paradigm from which this paradigm inherits.

Stems are composed with all of the slot entries, producing an FST that maps
from stems to all of their possible forms, with boundary symbols and features.
Thus, for a Latin first declension noun:

    aqu -> aqu+a[case=nom][num=sg]
    aqu -> aqu+ae[case=gen][num=sg]
    aqu -> aqu+am[case=acc][num=sg]

etc. Call this "stem_to_forms".

The Paradigm then defines a few common operations:

1. Analyzer:

This is the output projection of the stem_to_forms, composed with an FST that
deletes boundary labels and features, and then inverted. Thus this maps from,
e.g.:

    aquam -> aqu+am[case=acc][num=sg]

2. Tagger:

Same as the Analyzer, but omitting the decomposition of the word into parts:

    aquam -> aquam[case=acc][num=sg]

3. Lemmatizer:

The lemmatizer is constructed by taking stem_to_forms, moving the feature labels
to the input side, inverting (so it maps from fully-formed word to stem +
features), then inflecting the stem as the lemma. Thus:

    aquam -> aqua[case=acc][num=sg]

4. Inflector.

This is the inverse of the lemmatizer:

    aqua[case=acc][num=sg] -> aquam
"""

import array

from typing import Iterator, List, Optional, Sequence, Tuple

import pynini
from pynini.lib import byte
from pynini.lib import features
from pynini.lib import pynutil
from pynini.lib import rewrite


class Error(Exception):
  """Errors specific to this module."""

  pass


# Helper functions.


def build_stem_ids(min_id: int, max_id: int) -> pynini.Fst:
  """Builds the set of stem IDs.

  These are strings of the form __n__, for n in the range [min_id, max_id).

  Args:
    min_id: minimum stem ID (inclusive).
    max_id: maximum stem ID (exclusive).

  Returns:
    FST representing the stem IDs in [min_id, max_id) as strings.
  """
  return pynini.union(
      *["__{}__".format(i) for i in range(min_id, max_id)]).optimize()


def make_byte_star_except_boundary(
    boundary: pynini.FstLike = "+") -> pynini.Fst:
  """Helper function to make sigma-star over bytes, minus the boundary symbol.

  Args:
    boundary: a string, the boundary symbol to use.

  Returns:
    An acceptor representing sigma-star over bytes, minus the boundary symbol.
  """
  return pynini.difference(byte.BYTE, boundary).closure().optimize()


def prefix(affix: pynini.FstLike, stem_form: pynini.FstLike) -> pynini.Fst:
  """Helper function that preffixes `affix` to stem_form.

  Args:
    affix: an acceptor representing the affix, typically ending with the
      boundary symbol.
    stem_form: a transducer representing the stem. If there are no further
      restrictions on the form of the base, this could simply be the output of
      make_byte_star_except_boundary().  However the affix may impose a
      restriction on what it may attach to, or even impose a change of shape.

  Returns:
    The concatenation of the insertion of `affix` and `stem_form`.
  """
  return pynutil.insert(affix) + stem_form


def suffix(affix: pynini.FstLike, stem_form: pynini.FstLike) -> pynini.Fst:
  """Helper function that suffixes `affix` to stem_form.

  Args:
    affix: an acceptor representing the affix, typically beginning with the
      boundary symbol.
    stem_form: a transducer representing the stem. If there are no further
      restrictions on the form of the base, this could simply be the output of
      make_byte_star_except_boundary().  However the affix may impose a
      restriction on what it may attach to, or even impose a change of shape.

  Returns:
    The concatenation of `stem_form` and the insertion of `affix`.
  """
  return stem_form + pynutil.insert(affix)


# Type aliases used below.

Analysis = Tuple[str, features.FeatureVector]
ParadigmSlot = Tuple[pynini.Fst, features.FeatureVector]


class Paradigm:
  """Implementation of morphological paradigms.

  A paradigm consists of a Category, a sequence of slot (transducer,
  FeatureVector) pairs, a feature vector indicating which slot is the lemma,
  a sequence of stem acceptors or strings, an optional name,
  an optional set of rewrite rules to be applied in order,
  an optional boundary symbol and an optional parent paradigm from which to
  inherit.
  """

  def __init__(self,
               category: features.Category,
               slots: Sequence[ParadigmSlot],
               lemma_feature_vector: features.FeatureVector,
               stems: Sequence[pynini.FstLike],
               rules: Optional[Sequence[pynini.Fst]] = None,
               name: Optional[str] = None,
               boundary: str = "+",
               parent_paradigm: Optional["Paradigm"] = None):
    """Paradigm initializer.

    Args:
      category: a Category object.
      slots: a sequence of ParadigmSlots, or (fst, FeatureVector) pairs (the
        latter as a convenient shorthand).
      lemma_feature_vector: FeatureVector associated with the lemma. This must
        be one of the slots provided, otherwise the construction will fail.
      stems: a sequence of strings and/or acceptors representing stems
        belonging to this paradigm.
      rules: an optional sequence of FSTs, rules to be applied to produce the
        surface forms. If rules is None, then the rules are inherited from the
        parent category, if any.
      name: an optional string, the name of this paradigm.
      boundary: a string representing the boundary symbol.
      parent_paradigm: an optional Paradigm object from which to inherit.

    Raises:
        Error: Lemma form not found in slots.
    """
    self._category = category
    self._slots = list(slots)
    # Verify that the feature vectors are from the right category:
    for (_, feature_vector) in self._slots:
      if feature_vector.category != self._category:
        raise Error(f"Paradigm category {self._category} != "
                    f"feature vector category {feature_vector.category}")
    self._name = name
    self._boundary = boundary
    # Rule to delete the boundary symbol.
    self._boundary_deleter = self._unconditioned_rewrite(
        pynutil.delete(self.boundary))
    # Rule to delete the boundary label and feature labels.
    self._deleter = pynini.compose(
        self._unconditioned_rewrite(
            pynutil.delete(self.category._feature_labels)),
        self._boundary_deleter).optimize()
    # Rule to translate all boundary labels into human-readable strings.
    self._feature_label_rewriter = self._unconditioned_rewrite(
        self._category.feature_mapper)
    # And one that maps the other way
    self._feature_label_encoder = self._unconditioned_rewrite(
        pynini.invert(self._category.feature_mapper))
    # Inherit from the parent paradigm.
    self._rules = None if rules is None else list(rules)
    self._parent_paradigm = parent_paradigm
    self._inherit()
    # The union of all the shapes of the affixes in the slots, concatenated with
    # the insertion of the feature vectors.
    self._shape = pynini.union(*(
        (shape + pynutil.insert(feature_vector.acceptor))
        for (shape, feature_vector) in self._slots))
    # Derives the lemma form from the slot's shape, then delete all the features
    # and boundary symbol, so that it maps from the stem to the lemma without
    # the features and boundary symbol.
    self._lemma = None
    for (shape, feature_vector) in self._slots:
      if feature_vector == lemma_feature_vector:
        self._lemma = shape + pynutil.insert(lemma_feature_vector.acceptor)
    if self._lemma is None:
      raise Error("Lemma form not found in slots")
    if self._rules is not None:
      for rule in self._rules:
        self._lemma @= rule
    self._lemma @= self._deleter
    self._lemma.optimize()
    self._stems = list(stems)
    # Stems to form transducer.
    self._stems_to_forms = pynini.union(*self._stems)
    self._stems_to_forms.optimize()
    self._stems_to_forms @= self._shape
    if self._rules:
      for rule in self._rules:
        self._stems_to_forms @= rule
    self._stems_to_forms.optimize()
    # The analyzer, mapping from a fully formed word (e.g. "aquārum") to an
    # analysis (e.g. "aqu+ārum[case=gen][num=pl]")
    self._analyzer = None
    # The tagger, mapping from a fully formed word (e.g. "aquārum") to the
    # same string with morphosyntactic tags (e.g. "aquārum[case=gen][num=pl]").
    self._tagger = None
    # The lemmatizer, mapping from a fully formed word (e.g. "aquārum") to the
    # lemma with morphosyntactic tags (e.g. "aqua[case=gen][num=pl]").
    self._lemmatizer = None
    # Inversion of the lemmatizer.
    self._inflector = None

  def _inherit(self) -> None:
    """Inherit from parent paradigm.

    Checks that the categories and boundaries match, then sets the rules for
    this paradigm from the parent if they are None. Slots in the list provided
    to this paradigm then override any in the parent that have matching feature
    vectors.

    Raises:
        Error: Paradigm category/boundary != parent paradigm category/boundary.
    """
    if self._parent_paradigm is None:
      return
    if self._category != self._parent_paradigm.category:
      raise Error(f"Paradigm category {self._category} != "
                  f"parent paradigm category {self._parent_paradigm.category}")
    if self._boundary != self._parent_paradigm.boundary:
      raise Error(f"Paradigm boundary {self._boundary} != "
                  f"parent paradigm boundary {self._parent_paradigm.boundary}")
    if self._rules is None:
      self._rules = self._parent_paradigm.rules
    # Adds parent slots if their feature vector isn't in the current slots'
    # feature vector.
    self._slots.extend(
        parent_slot for parent_slot in self._parent_paradigm.slots
        if parent_slot[1] not in (slot[1] for slot in self._slots))

  def _flip_lemmatizer_feature_labels(self) -> None:
    """Destructively flips lemmatizer's feature labels from input to output."""
    feature_labels = set()
    for s in self.category.feature_labels.states():
      aiter = self.category.feature_labels.arcs(s)
      while not aiter.done():
        arc = aiter.value()
        if arc.ilabel:
          feature_labels.add(arc.ilabel)
        aiter.next()
    self.lemmatizer.set_input_symbols(self.lemmatizer.output_symbols())
    for s in self.lemmatizer.states():
      maiter = self.lemmatizer.mutable_arcs(s)
      while not maiter.done():
        arc = maiter.value()
        if arc.olabel in feature_labels:
          # This assertion should always be true by construction.
          assert arc.ilabel == 0, (
              f"ilabel = "
              f"{self.lemmatizer.input_symbols().find(arc.ilabel)},"
              f" olabel = "
              f"{self.lemmatizer.output_symbols().find(arc.olabel)}")
          arc = pynini.Arc(arc.olabel, arc.ilabel, arc.weight, arc.nextstate)
          maiter.set_value(arc)
        maiter.next()

  def _unconditioned_rewrite(self, tau: pynini.Fst) -> pynini.Fst:
    """Helper function for context-independent cdrewrites.

    Args:
      tau: Change FST, i.e. phi x psi.

    Returns:
      cdrewrite(tau, "", "", self._category.sigma_star)
    """
    return pynini.cdrewrite(tau, "", "", self._category.sigma_star).optimize()

  # Helper for parsing strings that contain stems and features.

  def _parse_lattice(self, lattice: pynini.Fst) -> Iterator[Analysis]:
    """Given a lattice, returns all string and feature vectors.

    Args:
      lattice: a rewrite lattice FST.

    Yields:
      Pairs of (string, feature vector).
    """
    gensym = pynini.generated_symbols()
    piter = lattice.paths()
    while not piter.done():
      # Mutable byte array for the stem, analysis string, or wordform.
      wordbuf = array.array("B")  # Mutable byte array.
      features_and_values = []
      for label in piter.olabels():
        if not label:
          continue
        if label < 256:
          wordbuf.append(label)
        else:
          features_and_values.append(gensym.find(label))
      word = wordbuf.tobytes().decode("utf8")
      vector = features.FeatureVector(self.category, *features_and_values)
      yield (word, vector)
      piter.next()

  # The analyzer, inflector, lemmatizer, and tagger are all created lazily.

  @property
  def analyzer(self) -> Optional[pynini.Fst]:
    if self.stems_to_forms is None:
      return None
    if self._analyzer is not None:
      return self._analyzer
    self._make_analyzer()
    return self._analyzer

  def _make_analyzer(self) -> None:
    """Helper function for constructing analyzer."""
    self._analyzer = pynini.project(self._stems_to_forms, "output")
    self._analyzer @= self._deleter
    self._analyzer.invert().optimize()

  def analyze(self, word: pynini.FstLike) -> List[Analysis]:
    """Returns list of possible analyses.

    Args:
      word: inflected form, the input to the analyzer.

    Returns:
      A list of possible analyses.

    Raises:
      rewrite.Error: composition failure.
    """
    return list(
        self._parse_lattice(rewrite.rewrite_lattice(word, self.analyzer)))

  @property
  def tagger(self) -> Optional[pynini.Fst]:
    if self.analyzer is None:
      return None
    if self._tagger is not None:
      return self._tagger
    self._make_tagger()
    return self._tagger

  def _make_tagger(self) -> None:
    """Helper function for constructing tagger."""
    self._tagger = self._analyzer @ self._boundary_deleter
    self._tagger.optimize()

  def tag(self, word: pynini.FstLike) -> List[Analysis]:
    """Returns list of possible taggings.

    Args:
      word: inflected form, the input to the tagger.

    Returns:
      A list of possible analysis.

    Raises:
      rewrite.Error: composition failure.
    """
    return list(self._parse_lattice(rewrite.rewrite_lattice(word, self.tagger)))

  @property
  def lemmatizer(self) -> Optional[pynini.Fst]:
    if self.stems_to_forms is None:
      return None
    if self._lemmatizer is not None:
      return self._lemmatizer
    self._make_lemmatizer()
    return self._lemmatizer

  def _make_lemmatizer(self) -> None:
    """Helper function for constructing lemmatizer."""
    # Breaking this down into steps for ease of readability.
    # Flips to map from form+features to stem.
    self._lemmatizer = self._stems_to_forms.copy()
    # Moves the feature labels to the input side, then invert. By construction
    # the feature labels are always concatenated to the end.
    self._flip_lemmatizer_feature_labels()
    # Deletes boundary on the analysis side.
    self._lemmatizer @= self._boundary_deleter
    self._lemmatizer.invert()
    # Maps from the stem side to the lemma. The self._feature_labels is needed
    # to match the features that are now glommed onto the right-hand side.
    self._lemmatizer @= self._lemma + self.category.feature_labels
    self._lemmatizer.optimize()

  def lemmatize(self, word: pynini.FstLike) -> List[Analysis]:
    """Returns list of possible lemmatizations.

    Args:
      word: inflected form, the input to the lemmatizer.

    Returns:
      A list of possible lemmatizations.

    Raises:
      rewrite.Error: composition failure.
    """
    return list(
        self._parse_lattice(rewrite.rewrite_lattice(word, self.lemmatizer)))

  @property
  def inflector(self) -> Optional[pynini.Fst]:
    if self.lemmatizer is None:
      return None
    if self._inflector is not None:
      return self._inflector
    self._inflector = pynini.invert(self._lemmatizer)
    return self._inflector

  def inflect(self, lemma: pynini.FstLike,
              featvec: features.FeatureVector) -> List[str]:
    """Returns list of possible inflections.

    Args:
      lemma: lemma string or acceptor.
      featvec: FeatureVector.

    Returns:
      A list of possible inflections.

    Raises:
      rewrite.Error: composition failure.
    """
    return rewrite.rewrites(lemma + featvec.acceptor, self.inflector)

  @property
  def category(self) -> features.Category:
    return self._category

  @property
  def slots(self) -> List[ParadigmSlot]:
    return self._slots

  @property
  def name(self) -> str:
    return self._name

  @property
  def boundary(self) -> str:
    return self._boundary

  @property
  def stems(self) -> List[pynini.FstLike]:
    return self._stems

  @property
  def stems_to_forms(self) -> pynini.Fst:
    return self._stems_to_forms

  @property
  def feature_label_rewriter(self) -> pynini.Fst:
    return self._feature_label_rewriter

  @property
  def feature_label_encoder(self) -> pynini.Fst:
    return self._feature_label_encoder

  @property
  def rules(self) -> Optional[List[pynini.Fst]]:
    return self._rules

