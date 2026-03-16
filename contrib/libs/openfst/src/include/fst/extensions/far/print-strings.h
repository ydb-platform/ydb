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
// Outputs as strings the string FSTs in a finite-state archive.

#ifndef FST_EXTENSIONS_FAR_PRINT_STRINGS_H_
#define FST_EXTENSIONS_FAR_PRINT_STRINGS_H_

#include <cstdint>
#include <iomanip>
#include <string>
#include <vector>

#include <fst/flags.h>
#include <fst/extensions/far/far.h>
#include <fstream>
#include <fst/shortest-distance.h>
#include <fst/string.h>

DECLARE_string(far_field_separator);

namespace fst {

template <class Arc>
void PrintStrings(FarReader<Arc> &reader, FarEntryType entry_type,
                  TokenType token_type, const std::string &begin_key,
                  const std::string &end_key, bool print_key, bool print_weight,
                  const std::string &symbols_source, bool initial_symbols,
                  int32_t generate_sources, const std::string &source_prefix,
                  const std::string &source_suffix) {
  std::unique_ptr<const SymbolTable> syms;
  if (!symbols_source.empty()) {
    // TODO(kbg): Allow negative flag?
    const SymbolTableTextOptions opts(true);
    syms.reset(SymbolTable::ReadText(symbols_source, opts));
    if (!syms) {
      LOG(ERROR) << "PrintStrings: Error reading symbol table "
                 << symbols_source;
      return;
    }
  }
  if (!begin_key.empty()) reader.Find(begin_key);
  std::string okey;
  int nrep = 0;
  for (int i = 1; !reader.Done(); reader.Next(), ++i) {
    const auto &key = reader.GetKey();
    if (!end_key.empty() && end_key < key) break;
    if (okey == key) {
      ++nrep;
    } else {
      nrep = 0;
    }
    okey = key;
    const auto *fst = reader.GetFst();
    if (i == 1 && initial_symbols && !syms && fst->InputSymbols()) {
      syms.reset(fst->InputSymbols()->Copy());
    }
    std::string str;
    VLOG(2) << "Handling key: " << key;
    const StringPrinter<Arc> printer(token_type,
                                     syms ? syms.get() : fst->InputSymbols(),
                                     /*omit_epsilon=*/false);
    printer(*fst, &str);
    if (entry_type == FarEntryType::LINE) {
      if (print_key)
        std::cout << key << FST_FLAGS_far_field_separator[0];
      std::cout << str;
      if (print_weight) {
        std::cout << FST_FLAGS_far_field_separator[0]
                  << ShortestDistance(*fst);
      }
      std::cout << std::endl;
    } else if (entry_type == FarEntryType::FILE) {
      std::stringstream sstrm;
      if (generate_sources) {
        sstrm.fill('0');
        sstrm << std::right << std::setw(generate_sources) << i;
      } else {
        sstrm << key;
        if (nrep > 0) sstrm << "." << nrep;
      }
      std::string source;
      source = source_prefix + sstrm.str() + source_suffix;
      std::ofstream ostrm(source);
      if (!ostrm) {
        LOG(ERROR) << "PrintStrings: Can't open file: " << source;
        return;
      }
      ostrm << str;
      if (token_type == TokenType::SYMBOL) ostrm << "\n";
    }
  }
}

}  // namespace fst

#endif  // FST_EXTENSIONS_FAR_PRINT_STRINGS_H_
