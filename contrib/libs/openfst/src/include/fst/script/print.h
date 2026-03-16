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

#ifndef FST_SCRIPT_PRINT_H_
#define FST_SCRIPT_PRINT_H_

#include <ostream>
#include <string>

#include <fst/flags.h>
#include <fst/script/fst-class.h>
#include <fst/script/print-impl.h>

DECLARE_string(fst_field_separator);

namespace fst {
namespace script {

// Note: it is safe to pass these strings as references because this struct is
// only used to pass them deeper in the call graph. Be sure you understand why
// this is so before using this struct for anything else!
struct FstPrintArgs {
  const FstClass &fst;
  const SymbolTable *isyms;
  const SymbolTable *osyms;
  const SymbolTable *ssyms;
  const bool accept;
  const bool show_weight_one;
  std::ostream &ostrm;
  const std::string &dest;
  const std::string &sep;
  const std::string &missing_symbol;
};

template <class Arc>
void Print(FstPrintArgs *args) {
  const Fst<Arc> &fst = *args->fst.GetFst<Arc>();
  FstPrinter<Arc> fstprinter(fst, args->isyms, args->osyms, args->ssyms,
                             args->accept, args->show_weight_one, args->sep,
                             args->missing_symbol);
  fstprinter.Print(args->ostrm, args->dest);
}

void Print(const FstClass &fst, std::ostream &ostrm, const std::string &dest,
           const SymbolTable *isyms = nullptr,
           const SymbolTable *osyms = nullptr,
           const SymbolTable *ssyms = nullptr, bool accept = true,
           bool show_weight_one = true, const std::string &missing_sym = "");

// TODO(kbg,2019-09-01): Deprecated.
void PrintFst(const FstClass &fst, std::ostream &ostrm, const std::string &dest,
              const SymbolTable *isyms, const SymbolTable *osyms,
              const SymbolTable *ssyms, bool accept, bool show_weight_one,
              const std::string &missing_sym = "");

// The same, but with more sensible defaults, and for arc-templated FSTs only.
// TODO(kbg,2019-09-01): Deprecated.
template <class Arc>
void PrintFst(const Fst<Arc> &fst, std::ostream &ostrm,
              const std::string &dest = "", const SymbolTable *isyms = nullptr,
              const SymbolTable *osyms = nullptr,
              const SymbolTable *ssyms = nullptr) {
  const std::string sep = FST_FLAGS_fst_field_separator.substr(0, 1);
  FstPrinter<Arc> fstprinter(fst, isyms, osyms, ssyms, true, true, sep);
  fstprinter.Print(ostrm, dest);
}

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_PRINT_H_
