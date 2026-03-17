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

#ifndef FST_SCRIPT_COMPILE_H_
#define FST_SCRIPT_COMPILE_H_

#include <istream>
#include <memory>
#include <string>

#include <fst/script/arg-packs.h>
#include <fst/script/compile-impl.h>
#include <fst/script/fst-class.h>

namespace fst {
namespace script {

// This operation exists in two forms. 1 is a void operation which writes the
// compiled machine to disk; 2 returns an FstClass. I/O should normally be done
// using the binary format for efficiency, so users are STRONGLY ENCOURAGED to
// use 1 or to construct FSTs using the C++ FST mutation operations.

// Note: it is safe to pass these strings as references because
// this struct is only used to pass them deeper in the call graph.
// Be sure you understand why this is so before using this struct
// for anything else!
struct FstCompileInnerArgs {
  std::istream &istrm;
  const std::string &source;
  const std::string &fst_type;
  const fst::SymbolTable *isyms;
  const fst::SymbolTable *osyms;
  const fst::SymbolTable *ssyms;
  const bool accep;
  const bool ikeep;
  const bool okeep;
  const bool nkeep;
  const bool allow_negative_labels;
};

using FstCompileArgs =
    WithReturnValue<std::unique_ptr<FstClass>, FstCompileInnerArgs>;

template <class Arc>
void CompileInternal(FstCompileArgs *args) {
  using fst::Convert;
  using fst::Fst;
  using fst::FstCompiler;
  FstCompiler<Arc> fstcompiler(
      args->args.istrm, args->args.source, args->args.isyms, args->args.osyms,
      args->args.ssyms, args->args.accep, args->args.ikeep, args->args.okeep,
      args->args.nkeep, args->args.allow_negative_labels);
  std::unique_ptr<Fst<Arc>> fst;
  if (args->args.fst_type != "vector") {
    std::unique_ptr<Fst<Arc>> tmp_fst(
        Convert<Arc>(fstcompiler.Fst(), args->args.fst_type));
    if (!tmp_fst) {
      FSTERROR() << "Failed to convert FST to desired type: "
                 << args->args.fst_type;
    }
    fst = std::move(tmp_fst);
  } else {
    fst = fst::WrapUnique(fstcompiler.Fst().Copy());
  }
  args->retval = fst ? std::make_unique<FstClass>(std::move(fst)) : nullptr;
}

void Compile(std::istream &istrm, const std::string &source,
             const std::string &dest, const std::string &fst_type,
             const std::string &arc_type, const SymbolTable *isyms,
             const SymbolTable *osyms, const SymbolTable *ssyms, bool accep,
             bool ikeep, bool okeep, bool nkeep, bool allow_negative_labels);

std::unique_ptr<FstClass> CompileInternal(
    std::istream &istrm, const std::string &source, const std::string &fst_type,
    const std::string &arc_type, const SymbolTable *isyms,
    const SymbolTable *osyms, const SymbolTable *ssyms, bool accep, bool ikeep,
    bool okeep, bool nkeep, bool allow_negative_labels);

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_COMPILE_H_
