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

#include <fst/extensions/pdt/getters.h>

namespace fst {
namespace script {

bool GetPdtComposeFilter(std::string_view str, PdtComposeFilter *cf) {
  if (str == "expand") {
    *cf = PdtComposeFilter::EXPAND;
  } else if (str == "expand_paren") {
    *cf = PdtComposeFilter::EXPAND_PAREN;
  } else if (str == "paren") {
    *cf = PdtComposeFilter::PAREN;
  } else {
    return false;
  }
  return true;
}

bool GetPdtParserType(std::string_view str, PdtParserType *pt) {
  if (str == "left") {
    *pt = PdtParserType::LEFT;
  } else if (str == "left_sr") {
    *pt = PdtParserType::LEFT_SR;
  } else {
    return false;
  }
  return true;
}

}  // namespace script
}  // namespace fst
