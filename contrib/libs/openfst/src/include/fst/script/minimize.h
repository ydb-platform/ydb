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

#ifndef FST_SCRIPT_MINIMIZE_H_
#define FST_SCRIPT_MINIMIZE_H_

#include <tuple>

#include <fst/minimize.h>
#include <fst/script/fst-class.h>

namespace fst {
namespace script {

using FstMinimizeArgs =
    std::tuple<MutableFstClass *, MutableFstClass *, float, bool>;

template <class Arc>
void Minimize(FstMinimizeArgs *args) {
  MutableFst<Arc> *ofst1 = std::get<0>(*args)->GetMutableFst<Arc>();
  MutableFst<Arc> *ofst2 =
      std::get<1>(*args) ? std::get<1>(*args)->GetMutableFst<Arc>() : nullptr;
  Minimize(ofst1, ofst2, std::get<2>(*args), std::get<3>(*args));
}

void Minimize(MutableFstClass *ofst1, MutableFstClass *ofst2 = nullptr,
              float delta = kShortestDelta, bool allow_nondet = false);

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_MINIMIZE_H_
