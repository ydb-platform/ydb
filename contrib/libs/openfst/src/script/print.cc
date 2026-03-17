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

#include <fst/script/print.h>

#include <string>

#include <fst/script/script-impl.h>

namespace fst {
namespace script {

void Print(const FstClass &fst, std::ostream &ostrm, const std::string &dest,
           const SymbolTable *isyms, const SymbolTable *osyms,
           const SymbolTable *ssyms, bool accept, bool show_weight_one,
           const std::string &missing_sym) {
  const auto sep = FST_FLAGS_fst_field_separator.substr(0, 1);
  FstPrintArgs args{fst,   isyms, osyms, ssyms,      accept, show_weight_one,
                    ostrm, dest,  sep,   missing_sym};
  Apply<Operation<FstPrintArgs>>("Print", fst.ArcType(), &args);
}

// TODO(kbg,2019-09-01): Deprecated.
void PrintFst(const FstClass &fst, std::ostream &ostrm, const std::string &dest,
              const SymbolTable *isyms, const SymbolTable *osyms,
              const SymbolTable *ssyms, bool accept, bool show_weight_one,
              const std::string &missing_sym) {
  Print(fst, ostrm, dest, isyms, osyms, ssyms, accept, show_weight_one,
        missing_sym);
}

REGISTER_FST_OPERATION_3ARCS(Print, FstPrintArgs);

}  // namespace script
}  // namespace fst
