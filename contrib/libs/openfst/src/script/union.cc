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

#include <fst/script/union.h>

#include <fst/script/script-impl.h>

namespace fst {
namespace script {

void Union(MutableFstClass *fst1, const FstClass &fst2) {
  if (!internal::ArcTypesMatch(*fst1, fst2, "Union")) {
    fst1->SetProperties(kError, kError);
    return;
  }
  FstUnionArgs1 args{fst1, fst2};
  Apply<Operation<FstUnionArgs1>>("Union", fst1->ArcType(), &args);
}

void Union(MutableFstClass *fst1, const std::vector<const FstClass *> &fsts2) {
  for (const auto *fst2 : fsts2) {
    if (!internal::ArcTypesMatch(*fst1, *fst2, "Union")) {
      fst1->SetProperties(kError, kError);
      return;
    }
  }
  FstUnionArgs2 args{fst1, fsts2};
  Apply<Operation<FstUnionArgs2>>("Union", fst1->ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(Union, FstUnionArgs1);
REGISTER_FST_OPERATION_3ARCS(Union, FstUnionArgs2);

}  // namespace script
}  // namespace fst
