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

#ifndef FST_SCRIPT_MAP_H_
#define FST_SCRIPT_MAP_H_

#include <cstdint>
#include <memory>
#include <tuple>

#include <fst/arc-map.h>
#include <fst/state-map.h>
#include <fst/script/arg-packs.h>
#include <fst/script/fst-class.h>
#include <fst/script/weight-class.h>

namespace fst {
namespace script {

template <class M>
std::unique_ptr<Fst<typename M::ToArc>> ArcMap(
    const Fst<typename M::FromArc> &fst, const M &mapper) {
  using ToArc = typename M::ToArc;
  auto ofst = std::make_unique<VectorFst<ToArc>>();
  ArcMap(fst, ofst.get(), mapper);
  return ofst;
}

template <class M>
std::unique_ptr<Fst<typename M::ToArc>> StateMap(
    const Fst<typename M::FromArc> &fst, const M &mapper) {
  using ToArc = typename M::ToArc;
  auto ofst = std::make_unique<VectorFst<ToArc>>();
  StateMap(fst, ofst.get(), mapper);
  return ofst;
}

enum class MapType : uint8_t {
  ARC_SUM,
  ARC_UNIQUE,
  IDENTITY,
  INPUT_EPSILON,
  INVERT,
  OUTPUT_EPSILON,
  PLUS,
  POWER,
  QUANTIZE,
  RMWEIGHT,
  SUPERFINAL,
  TIMES,
  TO_LOG,
  TO_LOG64,
  TO_STD
};

using FstMapInnerArgs =
    std::tuple<const FstClass &, MapType, float, double, const WeightClass &>;

using FstMapArgs = WithReturnValue<std::unique_ptr<FstClass>, FstMapInnerArgs>;

template <class Arc>
void Map(FstMapArgs *args) {
  using Weight = typename Arc::Weight;
  const Fst<Arc> &ifst = *std::get<0>(args->args).GetFst<Arc>();
  const auto map_type = std::get<1>(args->args);
  switch (map_type) {
    case MapType::ARC_SUM: {
      auto ofst = StateMap(ifst, ArcSumMapper<Arc>(ifst));
      args->retval = std::make_unique<FstClass>(std::move(ofst));
      return;
    }
    case MapType::ARC_UNIQUE: {
      auto ofst = StateMap(ifst, ArcUniqueMapper<Arc>(ifst));
      args->retval = std::make_unique<FstClass>(std::move(ofst));
      return;
    }
    case MapType::IDENTITY: {
      auto ofst = ArcMap(ifst, IdentityArcMapper<Arc>());
      args->retval = std::make_unique<FstClass>(std::move(ofst));
      return;
    }
    case MapType::INPUT_EPSILON: {
      auto ofst = ArcMap(ifst, InputEpsilonMapper<Arc>());
      args->retval = std::make_unique<FstClass>(std::move(ofst));
      return;
    }
    case MapType::INVERT: {
      auto ofst = ArcMap(ifst, InvertWeightMapper<Arc>());
      args->retval = std::make_unique<FstClass>(std::move(ofst));
      return;
    }
    case MapType::OUTPUT_EPSILON: {
      auto ofst = ArcMap(ifst, OutputEpsilonMapper<Arc>());
      args->retval = std::make_unique<FstClass>(std::move(ofst));
      return;
    }
    case MapType::PLUS: {
      const auto weight = *std::get<4>(args->args).GetWeight<Weight>();
      auto ofst = ArcMap(ifst, PlusMapper<Arc>(weight));
      args->retval = std::make_unique<FstClass>(std::move(ofst));
      return;
    }
    case MapType::POWER: {
      const auto power = std::get<3>(args->args);
      auto ofst = ArcMap(ifst, PowerMapper<Arc>(power));
      args->retval = std::make_unique<FstClass>(std::move(ofst));
      return;
    }
    case MapType::QUANTIZE: {
      const auto delta = std::get<2>(args->args);
      auto ofst = ArcMap(ifst, QuantizeMapper<Arc>(delta));
      args->retval = std::make_unique<FstClass>(std::move(ofst));
      return;
    }
    case MapType::RMWEIGHT: {
      auto ofst = ArcMap(ifst, RmWeightMapper<Arc>());
      args->retval = std::make_unique<FstClass>(std::move(ofst));
      return;
    }
    case MapType::SUPERFINAL: {
      auto ofst = ArcMap(ifst, SuperFinalMapper<Arc>());
      args->retval = std::make_unique<FstClass>(std::move(ofst));
      return;
    }
    case MapType::TIMES: {
      const auto weight = *std::get<4>(args->args).GetWeight<Weight>();
      auto ofst = ArcMap(ifst, TimesMapper<Arc>(weight));
      args->retval = std::make_unique<FstClass>(std::move(ofst));
      return;
    }
    case MapType::TO_LOG: {
      auto ofst = ArcMap(ifst, WeightConvertMapper<Arc, LogArc>());
      args->retval = std::make_unique<FstClass>(std::move(ofst));
      return;
    }
    case MapType::TO_LOG64: {
      auto ofst = ArcMap(ifst, WeightConvertMapper<Arc, Log64Arc>());
      args->retval = std::make_unique<FstClass>(std::move(ofst));
      return;
    }
    case MapType::TO_STD: {
      auto ofst = ArcMap(ifst, WeightConvertMapper<Arc, StdArc>());
      args->retval = std::make_unique<FstClass>(std::move(ofst));
      return;
    }
  }
}

std::unique_ptr<FstClass> Map(const FstClass &ifst, MapType map_type,
                              float delta, double power,
                              const WeightClass &weight);

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_MAP_H_
