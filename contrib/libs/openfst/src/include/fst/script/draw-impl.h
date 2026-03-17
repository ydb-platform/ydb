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
// Class to draw a binary FST by producing a text file in dot format, a helper
// class to fstdraw.cc.

#ifndef FST_SCRIPT_DRAW_IMPL_H_
#define FST_SCRIPT_DRAW_IMPL_H_

#include <iomanip>
#include <ostream>
#include <sstream>
#include <string>

#include <fst/fst.h>
#include <fst/util.h>
#include <fst/script/fst-class.h>
#include <string_view>

namespace fst {

// Print a binary FST in GraphViz textual format (helper class for fstdraw.cc).
// WARNING: Stand-alone use not recommend.
template <class Arc>
class FstDrawer {
 public:
  using Label = typename Arc::Label;
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;

  FstDrawer(const Fst<Arc> &fst, const SymbolTable *isyms,
            const SymbolTable *osyms, const SymbolTable *ssyms, bool accep,
            std::string_view title, float width, float height, bool portrait,
            bool vertical, float ranksep, float nodesep, int fontsize,
            int precision, std::string_view float_format,
            bool show_weight_one)
      : fst_(fst),
        isyms_(isyms),
        osyms_(osyms),
        ssyms_(ssyms),
        accep_(accep && fst.Properties(kAcceptor, true)),
        title_(title),
        width_(width),
        height_(height),
        portrait_(portrait),
        vertical_(vertical),
        ranksep_(ranksep),
        nodesep_(nodesep),
        fontsize_(fontsize),
        precision_(precision),
        float_format_(float_format),
        show_weight_one_(show_weight_one) {}

  // Draws FST to an output buffer.
  void Draw(std::ostream &strm, std::string_view dest) {
    SetStreamState(strm);
    dest_ = std::string(dest);
    const auto start = fst_.Start();
    if (start == kNoStateId) return;
    strm << "digraph FST {\n";
    if (vertical_) {
      strm << "rankdir = BT;\n";
    } else {
      strm << "rankdir = LR;\n";
    }
    strm << "size = \"" << width_ << "," << height_ << "\";\n";
    if (!title_.empty()) strm << "label = \"" + title_ + "\";\n";
    strm << "center = 1;\n";
    if (portrait_) {
      strm << "orientation = Portrait;\n";
    } else {
      strm << "orientation = Landscape;\n";
    }
    strm << "ranksep = \"" << ranksep_ << "\";\n"
         << "nodesep = \"" << nodesep_ << "\";\n";
    // Initial state first.
    DrawState(strm, start);
    for (StateIterator<Fst<Arc>> siter(fst_); !siter.Done(); siter.Next()) {
      const auto s = siter.Value();
      if (s != start) DrawState(strm, s);
    }
    strm << "}\n";
  }

 private:
  void SetStreamState(std::ostream &strm) const {
    strm << std::setprecision(precision_);
    if (float_format_ == "e") strm << std::scientific;
    if (float_format_ == "f") strm << std::fixed;
    // O.w. defaults to "g" per standard lib.
  }

  // Escapes backslash and double quote if these occur in the string. Dot
  // will not deal gracefully with these if they are not escaped.
  static std::string Escape(const std::string &str) {
    std::string ns;
    for (char c : str) {
      if (c == '\\' || c == '"') ns.push_back('\\');
      ns.push_back(c);
    }
    return ns;
  }

  std::string FormatId(StateId id, const SymbolTable *syms) const {
    if (syms) {
      auto symbol = syms->Find(id);
      if (symbol.empty()) {
        FSTERROR() << "FstDrawer: Integer " << id
                   << " is not mapped to any textual symbol"
                   << ", symbol table = " << syms->Name()
                   << ", destination = " << dest_;
        symbol = "?";
      }
      return Escape(symbol);
    } else {
      return std::to_string(id);
    }
  }

  std::string FormatStateId(StateId s) const { return FormatId(s, ssyms_); }

  std::string FormatILabel(Label label) const {
    return FormatId(label, isyms_);
  }

  std::string FormatOLabel(Label label) const {
    return FormatId(label, osyms_);
  }

  std::string FormatWeight(Weight w) const {
    std::stringstream ss;
    SetStreamState(ss);
    ss << w;
    // Weight may have double quote characters in it, so escape it.
    return Escape(ss.str());
  }

  void DrawState(std::ostream &strm, StateId s) const {
    strm << s << " [label = \"" << FormatStateId(s);
    const auto weight = fst_.Final(s);
    if (weight != Weight::Zero()) {
      if (show_weight_one_ || (weight != Weight::One())) {
        strm << "/" << FormatWeight(weight);
      }
      strm << "\", shape = doublecircle,";
    } else {
      strm << "\", shape = circle,";
    }
    if (s == fst_.Start()) {
      strm << " style = bold,";
    } else {
      strm << " style = solid,";
    }
    strm << " fontsize = " << fontsize_ << "]\n";
    for (ArcIterator<Fst<Arc>> aiter(fst_, s); !aiter.Done(); aiter.Next()) {
      const auto &arc = aiter.Value();
      strm << "\t" << s << " -> " << arc.nextstate << " [label = \""
           << FormatILabel(arc.ilabel);
      if (!accep_) {
        strm << ":" << FormatOLabel(arc.olabel);
      }
      if (show_weight_one_ || (arc.weight != Weight::One())) {
        strm << "/" << FormatWeight(arc.weight);
      }
      strm << "\", fontsize = " << fontsize_ << "];\n";
    }
  }

  const Fst<Arc> &fst_;
  const SymbolTable *isyms_;  // ilabel symbol table.
  const SymbolTable *osyms_;  // olabel symbol table.
  const SymbolTable *ssyms_;  // slabel symbol table.
  bool accep_;                // Print as acceptor when possible.
  std::string dest_;          // Drawn FST destination name.

  std::string title_;
  float width_;
  float height_;
  bool portrait_;
  bool vertical_;
  float ranksep_;
  float nodesep_;
  int fontsize_;
  int precision_;
  std::string float_format_;
  bool show_weight_one_;

  FstDrawer(const FstDrawer &) = delete;
  FstDrawer &operator=(const FstDrawer &) = delete;
};

}  // namespace fst

#endif  // FST_SCRIPT_DRAW_IMPL_H_
