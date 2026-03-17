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


#ifndef PYNINI_CROSSSCRIPT_H_
#define PYNINI_CROSSSCRIPT_H_

#include <tuple>
#include <utility>

#include <fst/script/fst-class.h>
#include "cross.h"

namespace fst {
namespace script {

using FstCrossArgs =
    std::tuple<const FstClass &, const FstClass &, MutableFstClass *>;

template <class Arc>
void Cross(FstCrossArgs *args) {
  const Fst<Arc> &ifst1 = *(std::get<0>(*args).GetFst<Arc>());
  const Fst<Arc> &ifst2 = *(std::get<1>(*args).GetFst<Arc>());
  MutableFst<Arc> *ofst = std::get<2>(*args)->GetMutableFst<Arc>();
  Cross(ifst1, ifst2, ofst);
}

void Cross(const FstClass &ifst1, const FstClass &ifst2, MutableFstClass *ofst);

}  // namespace script
}  // namespace fst

#endif  // PYNINI_CROSSSCRIPT_H_

