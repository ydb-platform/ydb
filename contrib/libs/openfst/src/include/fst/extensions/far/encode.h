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
#ifndef FST_EXTENSIONS_FAR_ENCODE_H_
#define FST_EXTENSIONS_FAR_ENCODE_H_

#include <fst/extensions/far/far.h>
#include <fst/extensions/far/getters.h>
#include <fst/extensions/far/map-reduce.h>
#include <fst/encode.h>
#include <fst/vector-fst.h>
#include <string_view>

namespace fst {

template <class Arc>
void Encode(FarReader<Arc> &reader, FarWriter<Arc> &writer,
            EncodeMapper<Arc> *mapper) {
  internal::Map(reader, writer,
                [mapper](std::string_view key, const Fst<Arc> *ifst) {
                  auto ofst = std::make_unique<VectorFst<Arc>>(*ifst);
                  Encode(ofst.get(), mapper);
                  return ofst;
                });
}

template <class Arc>
void Decode(FarReader<Arc> &reader, FarWriter<Arc> &writer,
            const EncodeMapper<Arc> &mapper) {
  internal::Map(reader, writer,
                [&mapper](std::string_view key, const Fst<Arc> *ifst) {
                  auto ofst = std::make_unique<VectorFst<Arc>>(*ifst);
                  Decode(ofst.get(), mapper);
                  return ofst;
                });
}

}  // namespace fst

#endif  // FST_EXTENSIONS_FAR_ENCODE_H_
