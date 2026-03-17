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

#include <fst/script/map.h>

#include <utility>

#include <fst/script/script-impl.h>

namespace fst {
namespace script {

std::unique_ptr<FstClass> Map(const FstClass &ifst, MapType map_type,
                              float delta, double power,
                              const WeightClass &weight) {
  if (!ifst.WeightTypesMatch(weight, "Map")) return nullptr;
  FstMapInnerArgs iargs{ifst, map_type, delta, power, weight};
  FstMapArgs args(iargs);
  Apply<Operation<FstMapArgs>>("Map", ifst.ArcType(), &args);
  return std::move(args.retval);
}

REGISTER_FST_OPERATION_3ARCS(Map, FstMapArgs);

}  // namespace script
}  // namespace fst
