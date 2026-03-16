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


#ifndef PYNINI_STRINGPRINTSCRIPT_H_
#define PYNINI_STRINGPRINTSCRIPT_H_

#include <string>

#include <fst/fst-decl.h>
#include <fst/script/arg-packs.h>
#include <fst/script/fstscript.h>
#include "stringprint.h"

namespace fst {
namespace script {

using StringPrintInnerArgs =
    std::tuple<const FstClass &, std::string *, TokenType, const SymbolTable *>;

using StringPrintArgs = WithReturnValue<bool, StringPrintInnerArgs>;

template <class Arc>
void StringPrint(StringPrintArgs *args) {
  const Fst<Arc> &fst = *(std::get<0>(args->args).GetFst<Arc>());
  args->retval = StringPrint(fst, std::get<1>(args->args),
                             std::get<2>(args->args), std::get<3>(args->args));
}

bool StringPrint(const FstClass &fst, std::string *str,
                 TokenType token_type = TokenType::BYTE,
                 const SymbolTable *symbols = nullptr);

}  // namespace script
}  // namespace fst

#endif  // PYNINI_STRINGPRINTSCRIPT_H_

