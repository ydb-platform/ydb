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

#ifndef FST_SCRIPT_UNION_H_
#define FST_SCRIPT_UNION_H_

#include <utility>
#include <vector>

#include <fst/union.h>
#include <fst/script/fst-class.h>

namespace fst {
namespace script {

using FstUnionArgs1 = std::pair<MutableFstClass *, const FstClass &>;

template <class Arc>
void Union(FstUnionArgs1 *args) {
  MutableFst<Arc> *fst1 = std::get<0>(*args)->GetMutableFst<Arc>();
  const Fst<Arc> &fst2 = *std::get<1>(*args).GetFst<Arc>();
  Union(fst1, fst2);
}

using FstUnionArgs2 =
    std::tuple<MutableFstClass *, const std::vector<const FstClass *> &>;

template <class Arc>
void Union(FstUnionArgs2 *args) {
  MutableFst<Arc> *fst1 = std::get<0>(*args)->GetMutableFst<Arc>();
  const auto &untyped_fsts2 = std::get<1>(*args);
  std::vector<const Fst<Arc> *> typed_fsts2;
  typed_fsts2.reserve(untyped_fsts2.size());
  for (const auto &untyped_fst2 : untyped_fsts2) {
    typed_fsts2.emplace_back(untyped_fst2->GetFst<Arc>());
  }
  Union(fst1, typed_fsts2);
}

void Union(MutableFstClass *fst1, const FstClass &fst2);

void Union(MutableFstClass *fst1, const std::vector<const FstClass *> &fsts2);

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_UNION_H_
