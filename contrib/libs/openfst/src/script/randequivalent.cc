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

#include <fst/script/randequivalent.h>

#include <cstdint>

#include <fst/script/script-impl.h>

namespace fst {
namespace script {

bool RandEquivalent(const FstClass &fst1, const FstClass &fst2, int32_t npath,
                    const RandGenOptions<RandArcSelection> &opts, float delta,
                    uint64_t seed) {
  if (!internal::ArcTypesMatch(fst1, fst2, "RandEquivalent")) return false;
  FstRandEquivalentInnerArgs iargs{fst1, fst2, npath, opts, delta, seed};
  FstRandEquivalentArgs args(iargs);
  Apply<Operation<FstRandEquivalentArgs>>("RandEquivalent", fst1.ArcType(),
                                          &args);
  return args.retval;
}

REGISTER_FST_OPERATION_3ARCS(RandEquivalent, FstRandEquivalentArgs);

}  // namespace script
}  // namespace fst
