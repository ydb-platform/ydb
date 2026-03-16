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

#ifndef FST_SCRIPT_EPSNORMALIZE_H_
#define FST_SCRIPT_EPSNORMALIZE_H_

#include <tuple>

#include <fst/epsnormalize.h>
#include <fst/script/fst-class.h>

namespace fst {
namespace script {

using FstEpsNormalizeArgs =
    std::tuple<const FstClass &, MutableFstClass *, EpsNormalizeType>;

template <class Arc>
void EpsNormalize(FstEpsNormalizeArgs *args) {
  const Fst<Arc> &ifst = *std::get<0>(*args).GetFst<Arc>();
  MutableFst<Arc> *ofst = std::get<1>(*args)->GetMutableFst<Arc>();
  EpsNormalize(ifst, ofst, std::get<2>(*args));
}

void EpsNormalize(const FstClass &ifst, MutableFstClass *ofst,
                  EpsNormalizeType norm_type = EPS_NORM_INPUT);

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_EPSNORMALIZE_H_
