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

#include <fst/script/shortest-distance.h>

#include <fst/script/script-impl.h>

namespace fst {
namespace script {

void ShortestDistance(const FstClass &fst, std::vector<WeightClass> *distance,
                      const ShortestDistanceOptions &opts) {
  FstShortestDistanceArgs1 args{fst, distance, opts};
  Apply<Operation<FstShortestDistanceArgs1>>("ShortestDistance", fst.ArcType(),
                                             &args);
}

void ShortestDistance(const FstClass &fst, std::vector<WeightClass> *distance,
                      bool reverse, double delta) {
  FstShortestDistanceArgs2 args{fst, distance, reverse, delta};
  Apply<Operation<FstShortestDistanceArgs2>>("ShortestDistance", fst.ArcType(),
                                             &args);
}

WeightClass ShortestDistance(const FstClass &fst, double delta) {
  FstShortestDistanceInnerArgs3 iargs{fst, delta};
  FstShortestDistanceArgs3 args(iargs);
  Apply<Operation<FstShortestDistanceArgs3>>("ShortestDistance", fst.ArcType(),
                                             &args);
  return args.retval;
}

REGISTER_FST_OPERATION_3ARCS(ShortestDistance, FstShortestDistanceArgs1);
REGISTER_FST_OPERATION_3ARCS(ShortestDistance, FstShortestDistanceArgs2);
REGISTER_FST_OPERATION_3ARCS(ShortestDistance, FstShortestDistanceArgs3);

}  // namespace script
}  // namespace fst
