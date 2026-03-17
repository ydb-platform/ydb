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


#include "stringcompilescript.h"

#include <string>

#include <fst/script/script-impl.h>

namespace fst {
namespace script {

bool StringCompile(const std::string &str, MutableFstClass *fst,
                   TokenType token_type, const SymbolTable *symbols,
                   const WeightClass &weight) {
  if (!fst->WeightTypesMatch(weight, "CompileSymbolString")) {
    fst->SetProperties(kError, kError);
    return false;
  }
  FstStringCompileInnerArgs iargs{str, fst, token_type, symbols, weight};
  FstStringCompileArgs args(iargs);
  Apply<Operation<FstStringCompileArgs>>("StringCompile", fst->ArcType(),
                                         &args);
  return args.retval;
}

REGISTER_FST_OPERATION_3ARCS(StringCompile, FstStringCompileArgs);

}  // namespace script
}  // namespace fst

