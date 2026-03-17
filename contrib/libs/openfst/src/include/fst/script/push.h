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

#ifndef FST_SCRIPT_PUSH_H_
#define FST_SCRIPT_PUSH_H_

#include <cstdint>
#include <tuple>

#include <fst/push.h>
#include <fst/script/fst-class.h>

namespace fst {
namespace script {

using FstPushArgs1 = std::tuple<MutableFstClass *, ReweightType, float, bool>;

template <class Arc>
void Push(FstPushArgs1 *args) {
  MutableFst<Arc> *fst = std::get<0>(*args)->GetMutableFst<Arc>();
  Push(fst, std::get<1>(*args), std::get<2>(*args), std::get<3>(*args));
}

using FstPushArgs2 = std::tuple<const FstClass &, MutableFstClass *, uint8_t,
                                ReweightType, float>;

template <class Arc>
void Push(FstPushArgs2 *args) {
  const Fst<Arc> &ifst = *std::get<0>(*args).GetFst<Arc>();
  MutableFst<Arc> *ofst = std::get<1>(*args)->GetMutableFst<Arc>();
  switch (std::get<3>(*args)) {
    case REWEIGHT_TO_FINAL: {
      Push<Arc, REWEIGHT_TO_FINAL>(ifst, ofst, std::get<2>(*args),
                                   std::get<4>(*args));
      return;
    }
    case REWEIGHT_TO_INITIAL: {
      Push<Arc, REWEIGHT_TO_INITIAL>(ifst, ofst, std::get<2>(*args),
                                     std::get<4>(*args));
      return;
    }
  }
}

void Push(MutableFstClass *fst, ReweightType type = REWEIGHT_TO_INITIAL,
          float delta = kShortestDelta, bool remove_total_weight = false);

void Push(const FstClass &ifst, MutableFstClass *ofst, uint8_t flags,
          ReweightType rew_type, float delta = kShortestDelta);

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_PUSH_H_
