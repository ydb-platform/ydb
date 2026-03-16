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

#ifndef FST_SCRIPT_ARCSORT_H_
#define FST_SCRIPT_ARCSORT_H_

#include <cstdint>
#include <utility>

#include <fst/arcsort.h>
#include <fst/script/fst-class.h>

namespace fst {
namespace script {

enum class ArcSortType : uint8_t { ILABEL, OLABEL };

using FstArcSortArgs = std::pair<MutableFstClass *, ArcSortType>;

template <class Arc>
void ArcSort(FstArcSortArgs *args) {
  MutableFst<Arc> *fst = std::get<0>(*args)->GetMutableFst<Arc>();
  switch (std::get<1>(*args)) {
    case ArcSortType::ILABEL: {
      const ILabelCompare<Arc> icomp;
      ArcSort(fst, icomp);
      return;
    }
    case ArcSortType::OLABEL: {
      const OLabelCompare<Arc> ocomp;
      ArcSort(fst, ocomp);
      return;
    }
  }
}

void ArcSort(MutableFstClass *ofst, ArcSortType);

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_ARCSORT_H_
