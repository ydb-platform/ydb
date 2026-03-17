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

#ifndef FST_SCRIPT_PRUNE_H_
#define FST_SCRIPT_PRUNE_H_

#include <cstdint>
#include <tuple>
#include <utility>

#include <fst/prune.h>
#include <fst/script/fst-class.h>
#include <fst/script/weight-class.h>

namespace fst {
namespace script {

using FstPruneArgs1 = std::tuple<const FstClass &, MutableFstClass *,
                                 const WeightClass &, int64_t, float>;

template <class Arc>
void Prune(FstPruneArgs1 *args) {
  using Weight = typename Arc::Weight;
  const Fst<Arc> &ifst = *std::get<0>(*args).GetFst<Arc>();
  MutableFst<Arc> *ofst = std::get<1>(*args)->GetMutableFst<Arc>();
  if constexpr (IsPath<Weight>::value) {
    const auto weight_threshold = *std::get<2>(*args).GetWeight<Weight>();
    Prune(ifst, ofst, weight_threshold, std::get<3>(*args), std::get<4>(*args));
  } else {
    FSTERROR() << "Prune: Weight must have path property: " << Weight::Type();
    ofst->SetProperties(kError, kError);
  }
}

using FstPruneArgs2 =
    std::tuple<MutableFstClass *, const WeightClass &, int64_t, float>;

template <class Arc>
void Prune(FstPruneArgs2 *args) {
  using Weight = typename Arc::Weight;
  MutableFst<Arc> *fst = std::get<0>(*args)->GetMutableFst<Arc>();
  if constexpr (IsPath<Weight>::value) {
    const auto weight_threshold = *std::get<1>(*args).GetWeight<Weight>();
    Prune(fst, weight_threshold, std::get<2>(*args), std::get<3>(*args));
  } else {
    FSTERROR() << "Prune: Weight must have path property: " << Weight::Type();
    fst->SetProperties(kError, kError);
  }
}

void Prune(const FstClass &ifst, MutableFstClass *ofst,
           const WeightClass &weight_threshold,
           int64_t state_threshold = kNoStateId, float delta = kDelta);

void Prune(MutableFstClass *fst, const WeightClass &weight_threshold,
           int64_t state_threshold = kNoStateId, float delta = kDelta);

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_PRUNE_H_
