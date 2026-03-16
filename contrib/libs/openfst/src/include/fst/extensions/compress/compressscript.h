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

#ifndef FST_EXTENSIONS_COMPRESS_COMPRESSSCRIPT_H_
#define FST_EXTENSIONS_COMPRESS_COMPRESSSCRIPT_H_

#include <string>
#include <tuple>

#include <fst/extensions/compress/compress.h>
#include <fst/script/arg-packs.h>
#include <fst/script/fst-class.h>

namespace fst {
namespace script {

using CompressInnerArgs = std::tuple<const FstClass &, const std::string &>;

using CompressArgs = WithReturnValue<bool, CompressInnerArgs>;

template <class Arc>
void Compress(CompressArgs *args) {
  const Fst<Arc> &fst = *std::get<0>(args->args).GetFst<Arc>();
  const auto &source = std::get<1>(args->args);
  args->retval = Compress(fst, source);
}

bool Compress(const FstClass &fst, const std::string &source);

using DecompressInnerArgs = std::tuple<const std::string &, MutableFstClass *>;

using DecompressArgs = WithReturnValue<bool, DecompressInnerArgs>;

template <class Arc>
void Decompress(DecompressArgs *args) {
  const auto &source = std::get<0>(args->args);
  MutableFst<Arc> *fst = std::get<1>(args->args)->GetMutableFst<Arc>();
  args->retval = Decompress(source, fst);
}

bool Decompress(const std::string &source, MutableFstClass *fst);

}  // namespace script
}  // namespace fst

#endif  // FST_EXTENSIONS_COMPRESS_COMPRESSSCRIPT_H_
