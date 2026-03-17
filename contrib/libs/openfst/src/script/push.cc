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

#include <fst/script/push.h>

#include <cstdint>

#include <fst/script/script-impl.h>

namespace fst {
namespace script {

void Push(MutableFstClass *fst, ReweightType rew_type, float delta,
          bool remove_total_weight) {
  FstPushArgs1 args{fst, rew_type, delta, remove_total_weight};
  Apply<Operation<FstPushArgs1>>("Push", fst->ArcType(), &args);
}

void Push(const FstClass &ifst, MutableFstClass *ofst, uint8_t flags,
          ReweightType rew_type, float delta) {
  if (!internal::ArcTypesMatch(ifst, *ofst, "Push")) {
    ofst->SetProperties(kError, kError);
    return;
  }
  FstPushArgs2 args{ifst, ofst, flags, rew_type, delta};
  Apply<Operation<FstPushArgs2>>("Push", ifst.ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(Push, FstPushArgs1);
REGISTER_FST_OPERATION_3ARCS(Push, FstPushArgs2);

}  // namespace script
}  // namespace fst
