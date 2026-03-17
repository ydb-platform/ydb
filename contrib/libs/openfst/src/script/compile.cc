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

#include <fst/script/compile.h>

#include <string>
#include <utility>

#include <fst/script/script-impl.h>

namespace fst {
namespace script {

void Compile(std::istream &istrm, const std::string &source,
             const std::string &dest, const std::string &fst_type,
             const std::string &arc_type, const SymbolTable *isyms,
             const SymbolTable *osyms, const SymbolTable *ssyms, bool accep,
             bool ikeep, bool okeep, bool nkeep, bool allow_negative_labels) {
  std::unique_ptr<FstClass> fst(
      CompileInternal(istrm, source, fst_type, arc_type, isyms, osyms, ssyms,
                      accep, ikeep, okeep, nkeep, allow_negative_labels));
  fst->Write(dest);
}

std::unique_ptr<FstClass> CompileInternal(
    std::istream &istrm, const std::string &source, const std::string &fst_type,
    const std::string &arc_type, const SymbolTable *isyms,
    const SymbolTable *osyms, const SymbolTable *ssyms, bool accep, bool ikeep,
    bool okeep, bool nkeep, bool allow_negative_labels) {
  FstCompileInnerArgs iargs{istrm,
                            source,
                            fst_type,
                            isyms,
                            osyms,
                            ssyms,
                            accep,
                            ikeep,
                            okeep,
                            nkeep,
                            allow_negative_labels};
  FstCompileArgs args(iargs);
  Apply<Operation<FstCompileArgs>>("CompileInternal", arc_type, &args);
  return std::move(args.retval);
}

// This registers form 2; 1 does not require registration.
REGISTER_FST_OPERATION_3ARCS(CompileInternal, FstCompileArgs);

}  // namespace script
}  // namespace fst
