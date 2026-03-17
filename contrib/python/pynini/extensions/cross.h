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


#ifndef PYNINI_CROSS_H_
#define PYNINI_CROSS_H_

#include <fst/arc-map.h>
#include <fst/compose.h>
#include <fst/fst.h>
#include <fst/mutable-fst.h>
#include <fst/rmepsilon.h>

namespace fst {

// This function combines two acceptors into a cross-product transducer; that
// if U accepts V_U and L accepts V_L, then their cross-product U x L accepts
// \forall v_u \in V_U, v_l \in V_L: v_u \rightarrow v_r. If called with a
// transducer for the first argument (the upper language), it will act as if it
// had already been projected onto its input, and if called with a transducer
// for the second argument (the lower language), it will act as if it had
// already been projected onto its output.
template <class Arc>
void Cross(const Fst<Arc> &ifst1, const Fst<Arc> &ifst2,
           MutableFst<Arc> *ofst) {
  static const ComposeOptions opts(/*connect=*/true,
                                   /*filter_type=*/MATCH_FILTER);
  static const OutputEpsilonMapper<Arc> oeps;
  static const InputEpsilonMapper<Arc> ieps;
  Compose(RmEpsilonFst<Arc>(ArcMapFst(ifst1, oeps)),
          RmEpsilonFst<Arc>(ArcMapFst(ifst2, ieps)), ofst, opts);
  // Copies symbol tables (if present).
  ofst->SetInputSymbols(ifst1.InputSymbols());
  ofst->SetOutputSymbols(ifst2.OutputSymbols());
}

}  // namespace fst

#endif  // PYNINI_CROSS_H_

