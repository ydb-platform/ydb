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

#include <fst/extensions/compress/compressscript.h>

#include <string>

#include <fst/arc-map.h>
#include <fst/script/script-impl.h>

namespace fst {
namespace script {

bool Compress(const FstClass &fst, const std::string &source) {
  CompressInnerArgs iargs(fst, source);
  CompressArgs args(iargs);
  Apply<Operation<CompressArgs>>("Compress", fst.ArcType(), &args);
  return args.retval;
}

REGISTER_FST_OPERATION_3ARCS(Compress, CompressArgs);

bool Decompress(const std::string &source, MutableFstClass *fst) {
  DecompressInnerArgs iargs(source, fst);
  DecompressArgs args(iargs);
  Apply<Operation<DecompressArgs>>("Decompress", fst->ArcType(), &args);
  return args.retval;
}

REGISTER_FST_OPERATION_3ARCS(Decompress, DecompressArgs);

}  // namespace script
}  // namespace fst
