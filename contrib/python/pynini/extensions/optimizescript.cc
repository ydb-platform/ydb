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


#include "optimizescript.h"

#include <fst/script/fst-class.h>
#include <fst/script/script-impl.h>

namespace fst {
namespace script {

void Optimize(MutableFstClass *fst, bool compute_props) {
  FstOptimizeArgs args{fst, compute_props};
  Apply<Operation<FstOptimizeArgs>>("Optimize", fst->ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(Optimize, FstOptimizeArgs);

void OptimizeDifferenceRhs(MutableFstClass *fst, bool compute_props) {
  FstOptimizeArgs args{fst, compute_props};
  Apply<Operation<FstOptimizeArgs>>("OptimizeDifferenceRhs", fst->ArcType(),
                                    &args);
}

REGISTER_FST_OPERATION_3ARCS(OptimizeDifferenceRhs, FstOptimizeArgs);

}  // namespace script
}  // namespace fst

