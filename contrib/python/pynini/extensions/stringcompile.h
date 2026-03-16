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


#ifndef PYNINI_STRINGCOMPILE_H_
#define PYNINI_STRINGCOMPILE_H_

#include <cstdint>
#include <iterator>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <fst/icu.h>
#include <fst/mutable-fst.h>
#include <fst/properties.h>
#include <fst/string.h>
#include <fst/symbol-table.h>

#include <fst/compat.h>
#include <string_view>
#include <optional>

// This module contains a singleton class which can compile strings into string
// FSTs, keeping track of so-called generated labels.
//
// As this is a singleton class, the standard way to access it is to
// define:
//
//   static StringCompiler *compiler = StringCompiler::Get();
//
// Input strings can be compiled by viewing them as raw bytes (BYTE),
// sequences of UTF-8-encoded Unicode codepoints (UTF8), or as a sequence of
// symbols in a predefined symbol table, delimited by whitespace (SYMBOL).
//
// Both the BYTE and UTF8 modes treat strings enclosed in square brackets as
// "generated symbols". Generated symbols are stored within the compiler
// singleton. They are assigned unique integral indices, beginning at 0xF0000.
// Therefore so if they are viewed as Unicode codepoints, they reside in the
// roughly 130,000 code points in planes 15-16 reserved for private use.
//
// The user can optionally attach a final weight to the resulting FST.
//
// This class also includes a const static method which returns a copy of
// a symbol table with the epsilon and the 95 printable ASCII characters.

namespace fst {

constexpr std::string_view kGeneratedSymbolsName = "**Generated symbols";
constexpr std::string_view kEpsilonString = "<epsilon>";

// Special handling for BOS and EOS markers in CDRewrite.
constexpr int64_t kBosIndex = 0xF8FE;
constexpr int64_t kEosIndex = 0xF8FF;
constexpr std::string_view kBosString = "BOS";
constexpr std::string_view kEosString = "EOS";

namespace internal {

// String compiler used by Pynini; used as a singleton.
class StringCompiler {
 public:
  static StringCompiler *Get();

  // Extracts a list of labels from an string. If token_type =
  // TokenType::SYMBOL, then the user must pass a symbol table used to label the
  // string.
  template <class Label>
  bool StringToLabels(std::string_view str, std::vector<Label> *labels,
                      TokenType token_type = TokenType::BYTE,
                      const SymbolTable *symbols = nullptr) {
    switch (token_type) {
      case TokenType::BYTE:
      case TokenType::UTF8: {
        bool inside_brackets = false;
        std::string chunk;
        for (auto it = str.begin(); it != str.end(); ++it) {
          char ch = *it;
          if (ch == '[') {
            if (inside_brackets) {
              LOG(ERROR) << "StringToLabels: Unmatched [";
              return false;
            }
            if (!ProcessUnbracketedSpan(chunk, labels,
                                        token_type == TokenType::BYTE)) {
              return false;
            }
            chunk.clear();
            inside_brackets = true;
          } else if (ch == ']') {
            if (!inside_brackets) {
              LOG(ERROR) << "StringToLabels: Unmatched ]";
              return false;
            }
            if (!ProcessBracketedSpan(chunk, labels)) return false;
            chunk.clear();
            inside_brackets = false;
          } else {
            if (ch == '\\') {
              // Final single and double backslash both compile to the same.
              if (it != std::prev(str.end())) ch = *(++it);
              switch (ch) {
                case 'n': {
                  ch = '\n';
                  break;
                }
                case 'r': {
                  ch = '\r';
                  break;
                }
                case 't': {
                  ch = '\t';
                  break;
                }
                case '[':
                case ']':
                case '\\': {
                  // Keeps these escaped character values the same (while
                  // dropping the backslash).
                  break;
                }
                default: {
                  chunk += '\\';
                }
              }
            }
            chunk += ch;
          }
        }
        if (inside_brackets) {
          LOG(ERROR) << "StringToLabels: Unmatched [";
          return false;
        }
        return ProcessUnbracketedSpan(chunk, labels,
                                      token_type == TokenType::BYTE);
      }
      case TokenType::SYMBOL: {
        // The empty string is valid.
        if (str.empty()) return true;
        for (const auto token : fst::StrSplit(str, ' ')) {
          const Label label = symbols->Find(token);
          if (label == kNoSymbol) {
            LOG(ERROR) << "SymbolStringToLabels: Symbol \"" << token << "\" "
                       << "is not mapped to any integer label in symbol table "
                       << symbols->Name();
            return false;
          }
          labels->emplace_back(label);
        }
        return true;
      }
    }
    return false;  // Unreachable.
  }

  // This method combines parsing strings into labels (StringToLabels) and
  // compilation of labels into a string FST (LabelsToFst).
  //
  // If token_type = TokenType::SYMBOL, then the user must pass a symbol table
  // used to label the string.
  template <class Arc>
  bool Compile(std::string_view str, MutableFst<Arc> *fst,
               TokenType token_type = TokenType::BYTE,
               const SymbolTable *symbols = nullptr,
               typename Arc::Weight weight = Arc::Weight::One()) {
    std::vector<typename Arc::Label> labels;
    if (!StringToLabels(str, &labels, token_type, symbols)) {
      LOG(ERROR) << "Failed to compile string `" << str << "`"
                 << ", with token_type: " << token_type;
      return false;
    }
    LabelsToFst(labels, fst, weight);
    return true;
  }

  // Returns a symbol table populated with the generated symbols.
  const SymbolTable &GeneratedSymbols() const { return generated_; }

  // Merges an existing `SymbolTable` of generated symbols (potentially from
  // another thread or from a file read on disk) and merges its generated
  // symbols into the generated symbols. This avoids conflicts between the two
  // for future symbol generation. A remapping for FSTs labeled using the given
  // generated SymbolTable will be populated during this run.
  bool MergeIntoGeneratedSymbols(const SymbolTable &symtab,
                                 std::map<int64_t, int64_t> *remap);
  // Resets `StringCompiler` to its state at construction.
  void Reset();

 private:
  // Standard constructor is private; other constructors are deleted. This
  // enforces the desired singleton pattern.
  StringCompiler();
  StringCompiler(const StringCompiler &) = delete;
  StringCompiler &operator=(const StringCompiler &) = delete;

  std::optional<int64_t> NumericalSymbolToLabel(std::string_view token) const;
  int64_t StringSymbolToLabel(std::string_view token);
  int64_t NumericalOrStringSymbolToLabel(std::string_view token);

  // Processes a BYTE or a UTF8 span inside brackets.
  template <class Label>
  bool ProcessBracketedSpan(std::string_view span,
                            std::vector<Label> *labels) {
    const std::vector<std::string_view> tokens = fst::StrSplit(span, ' ');
    if (tokens.empty()) {
      LOG(ERROR) << "ProcessBracketedSpan: Empty span";
      return false;
    } else if (tokens.size() == 1) {
      // Both numerical string parsing modes are available if there is a
      // single element in the bracketed span.
      labels->emplace_back(NumericalOrStringSymbolToLabel(tokens[0]));
    } else {
      // Only string parsing is available if there are multiple elements in
      // the bracketed span.
      for (const auto &token : tokens) {
        labels->emplace_back(StringSymbolToLabel(token));
      }
    }
    return true;
  }

  // Processes a BYTE or a UTF8 span outside brackets.
  template <class Label>
  bool ProcessUnbracketedSpan(std::string_view span,
                              std::vector<Label> *labels, bool byte) {
    return byte ? ByteStringToLabels(span, labels)
                : UTF8StringToLabels(span, labels);
  }

  template <class Arc>
  void LabelsToFst(const std::vector<typename Arc::Label> &labels,
                   MutableFst<Arc> *fst,
                   typename Arc::Weight weight = Arc::Weight::One()) {
    using Weight = typename Arc::Weight;
    fst->DeleteStates();
    auto s = fst->AddState();
    fst->SetStart(s);
    fst->AddStates(labels.size());
    for (const auto label : labels) {
      fst->AddArc(s, Arc(label, label, s + 1));
      ++s;
    }
    auto props = kCompiledStringProperties;
    if (weight == Weight::One()) {
      fst->SetFinal(s);
    } else {
      fst->SetFinal(s, std::move(weight));
      props &= ~kUnweighted;
      props |= kWeighted;
    }
    fst->SetProperties(props, props);
  }

  SymbolTable generated_;
  // The highest-numbered generated symbol currently present.
  int64_t max_generated_;
};

}  // namespace internal

// Convenience methods, to eliminate the need to call Get on the singleton.

const SymbolTable &GeneratedSymbols();

namespace thrax_internal {

bool MergeIntoGeneratedSymbols(const SymbolTable &symtab,
                               std::map<int64_t, int64_t> *remap);

void ResetGeneratedSymbols();

}  // namespace thrax_internal

template <class Label>
bool StringToLabels(std::string_view str,
                                         std::vector<Label> *labels,
                                         TokenType token_type = TokenType::BYTE,
                                         const SymbolTable *symbols = nullptr) {
  static auto *compiler = internal::StringCompiler::Get();
  return compiler->StringToLabels(str, labels, token_type, symbols);
}

template <class Arc>
bool StringCompile(
    std::string_view str, MutableFst<Arc> *fst,
    TokenType token_type = TokenType::BYTE,
    const SymbolTable *symbols = nullptr,
    typename Arc::Weight weight = Arc::Weight::One()) {
  static auto *compiler = internal::StringCompiler::Get();
  return compiler->Compile(str, fst, token_type, symbols, weight);
}

}  // namespace fst

#endif  // PYNINI_STRINGCOMPILE_H_

