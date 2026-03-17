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
#ifndef FST_EXTENSIONS_FAR_CONVERT_H_
#define FST_EXTENSIONS_FAR_CONVERT_H_

#include <fst/extensions/far/far.h>
#include <fst/extensions/far/getters.h>
#include <fst/extensions/far/map-reduce.h>
#include <fst/register.h>
#include <string_view>

namespace fst {

template <class Arc>
void Convert(FarReader<Arc> &reader, FarWriter<Arc> &writer,
             std::string_view fst_type) {
  internal::Map(reader, writer,
                [&fst_type](std::string_view key, const Fst<Arc> *ifst) {
                  if (fst_type.empty() || ifst->Type() == fst_type) {
                    return fst::WrapUnique(ifst->Copy());
                  }
                  auto ofst = fst::WrapUnique(Convert(*ifst, fst_type));
                  if (!ofst) {
                    FSTERROR() << "FarConvert: Cannot convert FST with key "
                               << key << " to " << fst_type;
                  }
                  return ofst;
                });
}

}  // namespace fst

#endif  // FST_EXTENSIONS_FAR_CONVERT_H_
