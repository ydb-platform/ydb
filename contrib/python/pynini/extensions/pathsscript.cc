// Copyright 2016-2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//


#include "pathsscript.h"

#include <fst/script/fst-class.h>
#include <fst/script/script-impl.h>

namespace fst {
namespace script {

StringPathIteratorClass::StringPathIteratorClass(
    const FstClass &fst, TokenType input_token_type,
    TokenType output_token_type, const SymbolTable *input_symbols,
    const SymbolTable *output_symbols)
    : impl_(nullptr) {
  InitStringPathIteratorClassArgs args{
      fst,           input_token_type, output_token_type,
      input_symbols, output_symbols,   this};
  Apply<Operation<InitStringPathIteratorClassArgs>>(
      "InitStringPathIteratorClass", fst.ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(InitStringPathIteratorClass,
                             InitStringPathIteratorClassArgs);

}  // namespace script
}  // namespace fst

