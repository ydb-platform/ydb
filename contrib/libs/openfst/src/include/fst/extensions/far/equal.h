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

#ifndef FST_EXTENSIONS_FAR_EQUAL_H_
#define FST_EXTENSIONS_FAR_EQUAL_H_

#include <fst/extensions/far/far.h>
#include <fst/extensions/far/map-reduce.h>
#include <fst/equal.h>
#include <string_view>

namespace fst {

template <class Arc>
bool Equal(FarReader<Arc> &reader1, FarReader<Arc> &reader2,
           float delta = kDelta, std::string_view begin_key = "",
           std::string_view end_key = "") {
  return internal::MapAllReduce(
      reader1, reader2,
      [delta](std::string_view key, const Fst<Arc> *fst1,
              const Fst<Arc> *fst2) {
        if (!Equal(*fst1, *fst2, delta)) {
          LOG(ERROR) << "Equal: FSTs for key " << key << " are not equal";
          return false;
        }
        return true;
      },
      begin_key, end_key);
}

}  // namespace fst

#endif  // FST_EXTENSIONS_FAR_EQUAL_H_
