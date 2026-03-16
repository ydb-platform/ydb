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


#ifndef PYNINI_STRINGCOMPILESCRIPT_H_
#define PYNINI_STRINGCOMPILESCRIPT_H_

#include <string>

#include <fst/script/arg-packs.h>
#include <fst/script/fst-class.h>
#include "stringcompile.h"

namespace fst {
namespace script {

using FstStringCompileInnerArgs =
    std::tuple<const std::string &, MutableFstClass *, TokenType,
               const SymbolTable *, const WeightClass &>;

using FstStringCompileArgs = WithReturnValue<bool, FstStringCompileInnerArgs>;

template <class Arc>
void StringCompile(FstStringCompileArgs *args) {
  MutableFst<Arc> *fst = std::get<1>(args->args)->GetMutableFst<Arc>();
  const typename Arc::Weight weight =
      *(std::get<4>(args->args).GetWeight<typename Arc::Weight>());
  args->retval =
      StringCompile(std::get<0>(args->args), fst, std::get<2>(args->args),
                    std::get<3>(args->args), weight);
}

// As is sometimes the case, there are fewer default arguments for the scripting
// API variant because we can't infer the underlying weight type.
bool StringCompile(const std::string &str, MutableFstClass *fst,
                   TokenType token_type, const SymbolTable *symbols,
                   const WeightClass &weight);

}  // namespace script
}  // namespace fst

#endif  // PYNINI_STRINGCOMPILESCRIPT_H_

