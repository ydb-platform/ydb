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

#include <fst/script/replace.h>

#include <cstdint>

#include <fst/script/script-impl.h>

namespace fst {
namespace script {

void Replace(const std::vector<std::pair<int64_t, const FstClass *>> &pairs,
             MutableFstClass *ofst, const ReplaceOptions &opts) {
  for (const auto &pair : pairs) {
    if (!internal::ArcTypesMatch(*pair.second, *ofst, "Replace")) {
      ofst->SetProperties(kError, kError);
      return;
    }
  }
  FstReplaceArgs args{pairs, ofst, opts};
  Apply<Operation<FstReplaceArgs>>("Replace", ofst->ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(Replace, FstReplaceArgs);

}  // namespace script
}  // namespace fst
