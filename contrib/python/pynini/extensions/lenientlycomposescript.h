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


#ifndef PYNINI_LENIENTLYCOMPOSESCRIPT_H_
#define PYNINI_LENIENTLYCOMPOSESCRIPT_H_

#include <tuple>
#include <utility>

#include <fst/script/fst-class.h>
#include "lenientlycompose.h"

namespace fst {
namespace script {

using FstLenientlyComposeArgs =
    std::tuple<const FstClass &, const FstClass &, const FstClass &,
               MutableFstClass *, const ComposeOptions &>;

template <class Arc>
void LenientlyCompose(FstLenientlyComposeArgs *args) {
  const Fst<Arc> &ifst1 = *(std::get<0>(*args).GetFst<Arc>());
  const Fst<Arc> &ifst2 = *(std::get<1>(*args).GetFst<Arc>());
  const Fst<Arc> &sigma = *(std::get<2>(*args).GetFst<Arc>());
  MutableFst<Arc> *ofst = std::get<3>(*args)->GetMutableFst<Arc>();
  LenientlyCompose(ifst1, ifst2, sigma, ofst, std::get<4>(*args));
}

void LenientlyCompose(const FstClass &ifst1, const FstClass &ifst2,
                      const FstClass &sigma, MutableFstClass *ofst,
                      const ComposeOptions &opts = ComposeOptions());

}  // namespace script
}  // namespace fst

#endif  // PYNINI_LENIENTLYCOMPOSESCRIPT_H_

