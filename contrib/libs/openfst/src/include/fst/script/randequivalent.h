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

#ifndef FST_SCRIPT_RANDEQUIVALENT_H_
#define FST_SCRIPT_RANDEQUIVALENT_H_

#include <cstdint>
#include <tuple>

#include <fst/randequivalent.h>
#include <fst/script/arg-packs.h>
#include <fst/script/fst-class.h>
#include <fst/script/script-impl.h>

namespace fst {
namespace script {

using FstRandEquivalentInnerArgs =
    std::tuple<const FstClass &, const FstClass &, int32_t,
               const RandGenOptions<RandArcSelection> &, float, uint64_t>;

using FstRandEquivalentArgs = WithReturnValue<bool, FstRandEquivalentInnerArgs>;

template <class Arc>
void RandEquivalent(FstRandEquivalentArgs *args) {
  const Fst<Arc> &fst1 = *std::get<0>(args->args).GetFst<Arc>();
  const Fst<Arc> &fst2 = *std::get<1>(args->args).GetFst<Arc>();
  const int32_t npath = std::get<2>(args->args);
  const auto &opts = std::get<3>(args->args);
  const float delta = std::get<4>(args->args);
  const uint64_t seed = std::get<5>(args->args);
  switch (opts.selector) {
    case RandArcSelection::UNIFORM: {
      const UniformArcSelector<Arc> selector(seed);
      const RandGenOptions<UniformArcSelector<Arc>> ropts(selector,
                                                          opts.max_length);
      args->retval = RandEquivalent(fst1, fst2, npath, ropts, delta, seed);
      return;
    }
    case RandArcSelection::FAST_LOG_PROB: {
      const FastLogProbArcSelector<Arc> selector(seed);
      const RandGenOptions<FastLogProbArcSelector<Arc>> ropts(selector,
                                                              opts.max_length);
      args->retval = RandEquivalent(fst1, fst2, npath, ropts, delta, seed);
      return;
    }
    case RandArcSelection::LOG_PROB: {
      const LogProbArcSelector<Arc> selector(seed);
      const RandGenOptions<LogProbArcSelector<Arc>> ropts(selector,
                                                          opts.max_length);
      args->retval = RandEquivalent(fst1, fst2, npath, ropts, delta, seed);
      return;
    }
  }
}

bool RandEquivalent(
    const FstClass &fst1, const FstClass &fst2, int32_t npath = 1,
    const RandGenOptions<RandArcSelection> &opts =
        RandGenOptions<RandArcSelection>(RandArcSelection::UNIFORM),
    float delta = kDelta, uint64_t seed = std::random_device()());

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_RANDEQUIVALENT_H_
