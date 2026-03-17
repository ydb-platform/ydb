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


#include "lenientlycomposescript.h"

#include <fst/script/script-impl.h>

namespace fst {
namespace script {

void LenientlyCompose(const FstClass &ifst1, const FstClass &ifst2,
                      const FstClass &sigma, MutableFstClass *ofst,
                      const ComposeOptions &opts) {
  if (!internal::ArcTypesMatch(ifst1, ifst2, "LenientlyCompose") ||
      !internal::ArcTypesMatch(ifst2, sigma, "LenientlyCompose") ||
      !internal::ArcTypesMatch(sigma, *ofst, "LenientlyCompose")) {
    ofst->SetProperties(kError, kError);
    return;
  }
  FstLenientlyComposeArgs args{ifst1, ifst2, sigma, ofst, opts};
  Apply<Operation<FstLenientlyComposeArgs>>("LenientlyCompose", ifst1.ArcType(),
                                            &args);
}

REGISTER_FST_OPERATION_3ARCS(LenientlyCompose, FstLenientlyComposeArgs);

}  // namespace script
}  // namespace fst

