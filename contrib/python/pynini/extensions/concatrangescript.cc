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


#include "concatrangescript.h"

#include <cstdint>

#include <fst/script/script-impl.h>

namespace fst {
namespace script {

void ConcatRange(MutableFstClass *fst, int32_t lower, int32_t upper) {
  FstConcatRangeArgs args{fst, lower, upper};
  Apply<Operation<FstConcatRangeArgs>>("ConcatRange", fst->ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(ConcatRange, FstConcatRangeArgs);

}  // namespace script
}  // namespace fst

