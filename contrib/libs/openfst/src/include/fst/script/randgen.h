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

#ifndef FST_SCRIPT_RANDGEN_H_
#define FST_SCRIPT_RANDGEN_H_

#include <cstdint>
#include <random>
#include <tuple>

#include <fst/randgen.h>
#include <fst/script/fst-class.h>
#include <fst/script/script-impl.h>

namespace fst {
namespace script {

using FstRandGenArgs =
    std::tuple<const FstClass &, MutableFstClass *,
               const RandGenOptions<RandArcSelection> &, uint64_t>;

template <class Arc>
void RandGen(FstRandGenArgs *args) {
  const Fst<Arc> &ifst = *std::get<0>(*args).GetFst<Arc>();
  MutableFst<Arc> *ofst = std::get<1>(*args)->GetMutableFst<Arc>();
  const auto &opts = std::get<2>(*args);
  const uint64_t seed = std::get<3>(*args);
  switch (opts.selector) {
    case RandArcSelection::UNIFORM: {
      const UniformArcSelector<Arc> selector(seed);
      const RandGenOptions<UniformArcSelector<Arc>> ropts(
          selector, opts.max_length, opts.npath, opts.weighted,
          opts.remove_total_weight);
      RandGen(ifst, ofst, ropts);
      return;
    }
    case RandArcSelection::FAST_LOG_PROB: {
      const FastLogProbArcSelector<Arc> selector(seed);
      const RandGenOptions<FastLogProbArcSelector<Arc>> ropts(
          selector, opts.max_length, opts.npath, opts.weighted,
          opts.remove_total_weight);
      RandGen(ifst, ofst, ropts);
      return;
    }
    case RandArcSelection::LOG_PROB: {
      const LogProbArcSelector<Arc> selector(seed);
      const RandGenOptions<LogProbArcSelector<Arc>> ropts(
          selector, opts.max_length, opts.npath, opts.weighted,
          opts.remove_total_weight);
      RandGen(ifst, ofst, ropts);
      return;
    }
  }
}

void RandGen(const FstClass &ifst, MutableFstClass *ofst,
             const RandGenOptions<RandArcSelection> &opts =
                 RandGenOptions<RandArcSelection>(RandArcSelection::UNIFORM),
             uint64_t seed = std::random_device()());

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_RANDGEN_H_
