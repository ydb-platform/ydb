// Copyright 2005-2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// See www.openfst.org for extensive documentation on this weighted
// finite-state transducer library.
//
// Reverses an MPDT.

#ifndef FST_EXTENSIONS_MPDT_REVERSE_H_
#define FST_EXTENSIONS_MPDT_REVERSE_H_

#include <limits>
#include <vector>

#include <fst/mutable-fst.h>
#include <fst/relabel.h>
#include <fst/reverse.h>

namespace fst {

// Reverses a multi-stack pushdown transducer (MPDT) encoded as an FST.
template <class Arc, class RevArc>
void Reverse(
    const Fst<Arc> &ifst,
    const std::vector<std::pair<typename Arc::Label, typename Arc::Label>>
        &parens,
    std::vector<typename Arc::Label> *assignments, MutableFst<RevArc> *ofst) {
  using Label = typename Arc::Label;
  // Reverses FST component.
  Reverse(ifst, ofst);
  // Exchanges open and close parenthesis pairs.
  std::vector<std::pair<Label, Label>> relabel_pairs;
  relabel_pairs.reserve(2 * parens.size());
  for (const auto &pair : parens) {
    relabel_pairs.emplace_back(pair.first, pair.second);
    relabel_pairs.emplace_back(pair.second, pair.first);
  }
  Relabel(ofst, relabel_pairs, relabel_pairs);
  // Computes new bounds for the stack assignments.
  Label max_level = -1;
  Label min_level = std::numeric_limits<Label>::max();
  for (const auto assignment : *assignments) {
    if (assignment < min_level) {
      min_level = assignment;
    } else if (assignment > max_level) {
      max_level = assignment;
    }
  }
  // Actually reverses stack assignments.
  for (auto &assignment : *assignments) {
    assignment = (max_level - assignment) + min_level;
  }
}

}  // namespace fst

#endif  // FST_EXTENSIONS_MPDT_REVERSE_H_
