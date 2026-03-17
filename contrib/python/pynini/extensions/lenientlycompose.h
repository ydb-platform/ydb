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


#ifndef PYNINI_LENIENTLYCOMPOSE_H_
#define PYNINI_LENIENTLYCOMPOSE_H_

// Lenient FST composition. For more information on this construction, see:
//
// Karttunen, L.. 1998. The proper treatment of Optimality Theory in
// computational phonology. In Proc. FSMNLP, pages 1-12.

#include <fst/compose.h>
#include <fst/connect.h>
#include <fst/determinize.h>
#include <fst/difference.h>
#include <fst/fst.h>
#include <fst/mutable-fst.h>
#include <fst/project.h>
#include <fst/rmepsilon.h>
#include <fst/union.h>
#include "checkprops.h"

namespace fst {
namespace internal {

// The priority union of two FSTs Q, R consists of the union of the relations in
// Q and R (as in vanilla union) subject to the constraint that the relations in
// Q have "priority". Imagine that:
//
//     Q(a) -> b
//     R(a) -> c
//
// Then, if U is the vanilla union of Q and R, U(a) -> {b, c}. But if P is the
// priority union of Q and R, U(a) -> b (not c).
//
// Here we compute the priority union of two FSTs with respect to sigma, a
// cyclic, unweighted acceptor representing the universal language. Then
// priority union is simply:
//
// func PriorityUnion[Q, R, sigma] {
//   input = Determinize[RmEpsilon[Project[Q, 'input']]];
//   return Q | ((sigma - input) @ R);
// }
template <class Arc>
void PriorityUnion(MutableFst<Arc> *fst1, const Fst<Arc> &fst2,
                   const Fst<Arc> &sigma) {
  if (!CheckUnweightedAcceptor(sigma, "PriorityUnion", "sigma")) {
    fst1->SetProperties(kError, kError);
    return;
  }
  const ProjectFst<Arc> project(*fst1, ProjectType::INPUT);
  const RmEpsilonFst<Arc> rmepsilon(project);
  const DeterminizeFst<Arc> determinize(rmepsilon);
  const DifferenceFst<Arc> difference(sigma, determinize);
  // We bail out if the contract for Difference was not satisfied.
  if (difference.Properties(kError, true) == kError) {
    fst1->SetProperties(kError, kError);
    return;
  }
  const ComposeFst<Arc> compose(difference, fst2);
  Union(fst1, compose);
}

}  // namespace internal

// Lenient composition of two FSTs X, Y is simply the priority union (with
// respect to some universal language) of the composition of X and Y with X.
// Thus it is a composition which gives priority to X @ Y, falling back upon X.
// Then lenient composition is simply:
//
// func LenientlyCompose[X, Y, sigma] {
//   return PriorityUnion[X @ Y, X, sigma];
// }
template <class Arc>
void LenientlyCompose(const Fst<Arc> &ifst1, const Fst<Arc> &ifst2,
                      const Fst<Arc> &sigma, MutableFst<Arc> *ofst,
                      const ComposeOptions &opts = ComposeOptions()) {
  Compose(ifst1, ifst2, ofst, opts);
  internal::PriorityUnion(ofst, ifst1, sigma);
  if (opts.connect) Connect(ofst);
}

}  // namespace fst

#endif  // PYNINI_LENIENTLYCOMPOSE_H_

