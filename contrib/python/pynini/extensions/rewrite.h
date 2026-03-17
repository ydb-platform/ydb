// Copyright 2016-2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//


#ifndef PYNINI_REWRITE_H_
#define PYNINI_REWRITE_H_

// Functions for applying rewrite rules to strings or FSTs. Unlike the naive
// approach, the lattices produced by composing the string and the FST rule
// are optimized (e.g., with epsilon-removal and pruned determinization) so
// that the output strings are unique.

#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <fst/compat.h>
#include <fst/log.h>
#include <fst/extensions/mpdt/compose.h>
#include <fst/extensions/pdt/compose.h>
#include <fst/arcsort.h>
#include <fst/compose.h>
#include <fst/determinize.h>
#include <fst/fst.h>
#include <fst/intersect.h>
#include <fst/minimize.h>
#include <fst/mutable-fst.h>
#include <fst/project.h>
#include <fst/rmepsilon.h>
#include <fst/shortest-path.h>
#include <fst/vector-fst.h>
#include "paths.h"
#include "stringprint.h"

// Generic rewrite utilities for string inputs.

namespace fst {
namespace internal {

// Post-composition check and cleanup.
template <class Arc>
inline bool CheckNonEmptyAndCleanup(MutableFst<Arc> *lattice) {
  if (lattice->Start() == kNoStateId) return false;
  // Projects output side if not already known to be an acceptor.
  if (lattice->Properties(kAcceptor, /*test=*/false) != kAcceptor) {
    Project(lattice, ProjectType::OUTPUT);
  }
  // Removes epsilons if not already known to be epsilon-free.
  if (lattice->Properties(kNoEpsilons, /*test=*/false) != kNoEpsilons) {
    RmEpsilon(lattice);
  }
  return true;
}

}  // namespace internal

// Constructs a weighted, epsilon-free lattice of output strings given a
// input FST and a rule FST.
//
// Callers may wish to arc-sort the input side of the rule ahead of time.
template <class Arc>
bool RewriteLattice(const Fst<Arc> &input, const Fst<Arc> &rule,
                    MutableFst<Arc> *lattice) {
  static const ComposeOptions opts(true, ALT_SEQUENCE_FILTER);
  Compose(input, rule, lattice, opts);
  return internal::CheckNonEmptyAndCleanup(lattice);
}

// Same, but supports PDT composition.
template <class Arc>
bool RewriteLattice(
    const Fst<Arc> &input, const Fst<Arc> &rule, MutableFst<Arc> *lattice,
    const std::vector<std::pair<typename Arc::Label, typename Arc::Label>>
        &pdt_parens) {
  static const PdtComposeOptions opts(true, PdtComposeFilter::EXPAND);
  Compose(input, rule, pdt_parens, lattice, opts);
  return internal::CheckNonEmptyAndCleanup(lattice);
}

// Same, but supports MPDT composition.
template <class Arc>
bool RewriteLattice(
    const Fst<Arc> &input, const Fst<Arc> &rule, MutableFst<Arc> *lattice,
    const std::vector<std::pair<typename Arc::Label, typename Arc::Label>>
        &pdt_parens,
    const std::vector<typename Arc::Label> &mpdt_assignments) {
  static const MPdtComposeOptions opts(true, PdtComposeFilter::EXPAND);
  Compose(input, rule, pdt_parens, mpdt_assignments, lattice, opts);
  return internal::CheckNonEmptyAndCleanup(lattice);
}

// Given an epsilon-free lattice of output strings (such as produced by
// RewriteLattice), attempts to determinize it, pruning non-optimal paths if
// `optimal_only` is true. This is only valid in a semiring with the path
// property.
//
// To prevent unexpected blowup during determinization, a state threshold is
// also used and a warning is logged if this exact threshold is reached. The
// threshold is a multiplier of the size of the input lattice (by default, 4),
// plus a small constant factor. This is intended to be a sensible default
// and is not an inherently meaningful value in and of itself.
template <class Arc>
void LatticeToDfa(MutableFst<Arc> *lattice, bool optimal_only,
                  typename Arc::StateId state_multiplier = 4) {
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;
  const auto &weight_threshold = optimal_only ? Weight::One() : Weight::Zero();
  const StateId state_threshold = 256 + state_multiplier * lattice->NumStates();
  const DeterminizeOptions<Arc> opts(kDelta, weight_threshold, state_threshold);
  Determinize(*lattice, lattice, opts);
  // Warns if we actually hit the state threshold; if so, we do not have the
  // full set of (optimal) rewrites; there may be cycles of unweighted
  // insertions, or the state threshold may just be too low.
  if (lattice->NumStates() == state_threshold) {
    LOG(WARNING) << "Unexpectedly hit state threshold; consider a higher value "
                    "for state_multiplier";
  }
}

// Given an epsilon-free lattice of output strings (such as produced by
// RewriteLattice), extracts n-shortest unique strings. This is only valid in a
// semiring with the path property.
template <class Arc>
void LatticeToShortest(MutableFst<Arc> *lattice, int32_t nshortest = 1) {
  VectorFst<Arc> shortest;
  // By requesting unique solutions we request on-the-fly determinization.
  ShortestPath(*lattice, &shortest, nshortest, /*unique=*/true);
  *lattice = shortest;
}

// Given an epsilon-free lattice of output strings (such as produced by
// RewriteLattice), extracts a single top string. This is only valid in a
// semiring with the path property.
template <class Arc>
bool LatticeToTopString(const Fst<Arc> &lattice, std::string *output,
                        TokenType ttype = TokenType::BYTE,
                        const SymbolTable *syms = nullptr) {
  VectorFst<Arc> ofst;
  ShortestPath(lattice, &ofst);
  return StringPrint(ofst, output, ttype, syms);
}

// Attempts to extract a single top rewrite from a optimized DFA, logging a
// warning and returning false if there's a tie. This is only valid in a
// semiring with the path property.
template <class Arc>
bool LatticeToOneTopString(const Fst<Arc> &lattice, std::string *output,
                           TokenType ttype = TokenType::BYTE,
                           const SymbolTable *syms = nullptr) {
  StringPathIterator<Arc> paths(lattice, ttype, syms, /*check_acyclic=*/false);
  if (paths.Error() || paths.Done()) return false;
  *output = paths.OString();
  // Checks for uniqueness.
  paths.Next();
  if (!paths.Done()) {
    LOG(ERROR) << "Multiple top rewrites found: '" << *output << "' and '"
               << paths.OString() << "' (weight: " << paths.Weight() << ")";
    return false;
  }
  return true;
}

// Clears vector and writes lattice labels to it.
template <class Arc>
bool LatticeToLabels(const Fst<Arc> &lattice,
                     std::vector<std::vector<typename Arc::Label>> *output) {
  output->clear();
  PathIterator<Arc> paths(lattice);
  if (paths.Error()) return false;
  for (; !paths.Done(); paths.Next()) output->emplace_back(paths.OLabels());
  return true;
}

// Clears vector and writes lattice strings to it.
template <class Arc>
bool LatticeToStrings(const Fst<Arc> &lattice, std::vector<std::string> *output,
                      TokenType ttype = TokenType::BYTE,
                      const SymbolTable *syms = nullptr) {
  output->clear();
  // We have to do this check manually since StringPathIterator's check is
  // potentially fatal.
  if (lattice.Properties(kAcyclic, true) != kAcyclic) {
    LOG(ERROR) << "Lattice is unexpectedly cyclic";
    return false;
  }
  // Input token type and symbol table will be ignored.
  StringPathIterator<Arc> paths(lattice, ttype, syms, /*check_acyclic=*/false);
  if (paths.Error()) return false;
  for (; !paths.Done(); paths.Next()) {
    // Constructs these in-place.
    output->emplace_back();
    paths.OString(&output->back());
  }
  return true;
}

// Top rewrite.
template <class Arc>
bool TopRewrite(const Fst<Arc> &input, const Fst<Arc> &rule,
                std::string *output, TokenType ttype = TokenType::BYTE,
                const SymbolTable *syms = nullptr) {
  VectorFst<Arc> lattice;
  return RewriteLattice(input, rule, &lattice) &&
         LatticeToTopString(lattice, output, ttype, syms);
}

// Top rewrite, returning false and logging if there's a tie.
template <class Arc>
bool OneTopRewrite(const Fst<Arc> &input, const Fst<Arc> &rule,
                   std::string *output, TokenType ttype = TokenType::BYTE,
                   const SymbolTable *syms = nullptr,
                   typename Arc::StateId state_multiplier = 4) {
  VectorFst<Arc> lattice;
  if (!RewriteLattice(input, rule, &lattice)) return false;
  LatticeToDfa(&lattice, /*optimal_only=*/true, state_multiplier);
  return LatticeToOneTopString(lattice, output, ttype, syms);
}

// All rewrites.
template <class Arc>
bool Rewrites(const Fst<Arc> &input, const Fst<Arc> &rule,
              std::vector<std::string> *output,
              TokenType ttype = TokenType::BYTE,
              const SymbolTable *syms = nullptr,
              typename Arc::StateId state_multiplier = 4) {
  VectorFst<Arc> lattice;
  if (!RewriteLattice(input, rule, &lattice)) return false;
  LatticeToDfa(&lattice, /*optimal_only=*/false, state_multiplier);
  return LatticeToStrings(lattice, output, ttype, syms);
}

// The same, but with repeated string fields.
// All optimal rewrites.
template <class Arc>
bool TopRewrites(const Fst<Arc> &input, const Fst<Arc> &rule,
                 std::vector<std::string> *output,
                 TokenType ttype = TokenType::BYTE,
                 const SymbolTable *syms = nullptr,
                 typename Arc::StateId state_multiplier = 4) {
  VectorFst<Arc> lattice;
  if (!RewriteLattice(input, rule, &lattice)) return false;
  LatticeToDfa(&lattice, /*optimal_only=*/true, state_multiplier);
  return LatticeToStrings(lattice, output, ttype, syms);
}

// The same, but with repeated string fields.
// The top n rewrites.
template <class Arc>
bool TopRewrites(const Fst<Arc> &input, const Fst<Arc> &rule, int32_t nshortest,
                 std::vector<std::string> *output,
                 TokenType ttype = TokenType::BYTE,
                 const SymbolTable *syms = nullptr) {
  VectorFst<Arc> lattice;
  if (!RewriteLattice(input, rule, &lattice)) return false;
  LatticeToShortest(&lattice, nshortest);
  return LatticeToStrings(lattice, output, ttype, syms);
}

// The same, but with repeated string fields.
// Determines whether a rule allows an input/output pair.
template <class Arc>
bool Matches(const Fst<Arc> &input, const Fst<Arc> &output,
             const Fst<Arc> &rule) {
  VectorFst<Arc> lattice;
  if (!RewriteLattice(input, rule, &lattice)) return false;
  static const OLabelCompare<Arc> ocomp;
  ArcSort(&lattice, ocomp);
  static const IntersectOptions opts(true, SEQUENCE_FILTER);
  Intersect(lattice, output, &lattice, opts);
  return lattice.Start() != kNoStateId;
}

}  // namespace fst

#endif  // PYNINI_REWRITE_H_

