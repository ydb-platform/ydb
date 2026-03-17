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
//
// Definitions and functions for invoking and using Far main functions that
// support multiple and extensible arc types.

#include <fst/extensions/far/script-impl.h>

#include <string>

#include <fst/extensions/far/far.h>
#include <fstream>

namespace fst {
namespace script {

std::string LoadArcTypeFromFar(const std::string &far_source) {
  FarHeader hdr;
  if (!hdr.Read(far_source)) {
    LOG(ERROR) << "Error reading FAR: " << far_source;
    return "";
  }
  return hdr.ArcType();
}

std::string LoadArcTypeFromFst(const std::string &fst_source) {
  FstHeader hdr;
  std::ifstream in(fst_source, std::ios_base::in | std::ios_base::binary);
  if (!hdr.Read(in, fst_source)) {
    LOG(ERROR) << "Error reading FST: " << fst_source;
    return "";
  }
  return hdr.ArcType();
}

}  // namespace script
}  // namespace fst
