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
// Stand-alone class to print out binary FSTs in the AT&T format, a helper
// class for fstprint.cc.

#ifndef FST_SCRIPT_PRINT_IMPL_H_
#define FST_SCRIPT_PRINT_IMPL_H_

#include <ostream>
#include <sstream>
#include <string>

#include <fst/fstlib.h>
#include <fst/util.h>

namespace fst {

// Print a binary FST in textual format (helper class for fstprint.cc).
// WARNING: Stand-alone use of this class not recommended, most code should
// read/write using the binary format which is much more efficient.
template <class Arc>
class FstPrinter {
 public:
  using StateId = typename Arc::StateId;
  using Label = typename Arc::Label;
  using Weight = typename Arc::Weight;

  explicit FstPrinter(const Fst<Arc> &fst, const SymbolTable *isyms,
                      const SymbolTable *osyms, const SymbolTable *ssyms,
                      bool accept, bool show_weight_one,
                      const std::string &field_separator,
                      const std::string &missing_symbol = "")
      : fst_(fst),
        isyms_(isyms),
        osyms_(osyms),
        ssyms_(ssyms),
        accept_(accept && (fst.Properties(kAcceptor, true) == kAcceptor)),
        show_weight_one_(show_weight_one),
        sep_(field_separator),
        missing_symbol_(missing_symbol) {}

  // Prints FST to an output stream.
  void Print(std::ostream &ostrm, const std::string &dest) {
    dest_ = dest;
    const auto start = fst_.Start();
    if (start == kNoStateId) return;
    // Initial state first.
    PrintState(ostrm, start);
    for (StateIterator<Fst<Arc>> siter(fst_); !siter.Done(); siter.Next()) {
      const auto s = siter.Value();
      if (s != start) PrintState(ostrm, s);
    }
  }

 private:
  std::string FormatId(StateId id, const SymbolTable *syms) const {
    if (syms) {
      std::string symbol = syms->Find(id);
      if (symbol.empty()) {
        if (missing_symbol_.empty()) {
          FSTERROR() << "FstPrinter: Integer " << id
                     << " is not mapped to any textual symbol"
                     << ", symbol table = " << syms->Name()
                     << ", destination = " << dest_;
          symbol = "?";
        } else {
          symbol = missing_symbol_;
        }
      }
      return symbol;
    } else {
      return std::to_string(id);
    }
  }

  std::string FormatStateId(StateId s) const { return FormatId(s, ssyms_); }

  std::string FormatILabel(Label l) const { return FormatId(l, isyms_); }

  std::string FormatOLabel(Label l) const { return FormatId(l, osyms_); }

  void PrintState(std::ostream &ostrm, StateId s) const {
    bool output = false;
    for (ArcIterator<Fst<Arc>> aiter(fst_, s); !aiter.Done(); aiter.Next()) {
      const auto &arc = aiter.Value();
      ostrm << FormatStateId(s) << sep_ << FormatStateId(arc.nextstate)
              << sep_ << FormatILabel(arc.ilabel);
      if (!accept_) {
        ostrm << sep_ << FormatOLabel(arc.olabel);
      }
      if (show_weight_one_ || arc.weight != Weight::One()) {
        ostrm << sep_ << arc.weight;
      }
      ostrm << "\n";
      output = true;
    }
    const auto weight = fst_.Final(s);
    if (weight != Weight::Zero() || !output) {
      ostrm << FormatStateId(s);
      if (show_weight_one_ || weight != Weight::One()) {
        ostrm << sep_ << weight;
      }
      ostrm << "\n";
    }
  }

  const Fst<Arc> &fst_;
  const SymbolTable *isyms_;    // ilabel symbol table.
  const SymbolTable *osyms_;    // olabel symbol table.
  const SymbolTable *ssyms_;    // slabel symbol table.
  bool accept_;                 // Print as acceptor when possible?
  std::string dest_;            // Text FST destination name.
  bool show_weight_one_;        // Print weights equal to Weight::One()?
  std::string sep_;             // Separator character between fields.
  std::string missing_symbol_;  // Symbol to print when lookup fails (default
                                // "" means raise error).

  FstPrinter(const FstPrinter &) = delete;
  FstPrinter &operator=(const FstPrinter &) = delete;
};

}  // namespace fst

#endif  // FST_SCRIPT_PRINT_IMPL_H_
