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


#ifndef PYNINI_STRINGMAP_H_
#define PYNINI_STRINGMAP_H_

// This file contains functions for compiling FSTs from pairs of strings
// using a prefix tree.

#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <fstream>
#include <fst/mutable-fst.h>
#include <fst/string.h>
#include <fst/symbol-table.h>
#include "prefix_tree.h"
#include "stringcompile.h"
#include "stringfile.h"

#include <fst/compat.h>
#include <string_view>

namespace fst {
namespace internal {

// Helper class for constructing string maps.
template <class Arc, class PTree>
class StringMapCompiler {
 public:
  using Label = typename Arc::Label;
  using Weight = typename Arc::Weight;

  explicit StringMapCompiler(TokenType input_token_type = TokenType::BYTE,
                             TokenType output_token_type = TokenType::BYTE,
                             const SymbolTable *input_symbols = nullptr,
                             const SymbolTable *output_symbols = nullptr)
      : input_token_type_(input_token_type),
        output_token_type_(output_token_type),
        input_symbols_(input_symbols),
        output_symbols_(output_symbols) {}

  // One-string version.
  bool Add(std::string_view iostring) {
    return Add(iostring, iostring, Weight::One());
  }

  // Two-string version.
  bool Add(std::string_view istring, std::string_view ostring,
           Weight weight = Weight::One()) {
    std::vector<Label> ilabels;
    if (!StringToLabels(istring, &ilabels, input_token_type_, input_symbols_))
      return false;
    std::vector<Label> olabels;
    if (!StringToLabels(ostring, &olabels, output_token_type_, output_symbols_))
      return false;
    ptree_.Add(ilabels, olabels, std::move(weight));
    return true;
  }

  // Three-string version, which also requires us to parse the weight.
  bool Add(std::string_view istring, std::string_view ostring,
           std::string_view wstring) {
    std::istringstream strm{std::string(wstring)};
    Weight weight;
    strm >> weight;
    if (!strm) {
       LOG(ERROR) << "StringMapCompiler::Add: Bad weight: " << wstring;
       return false;
    }
    return Add(istring, ostring, std::move(weight));
  }

  void Compile(MutableFst<Arc> *fst) const { ptree_.ToFst(fst); }

 private:
  const TokenType input_token_type_;
  const TokenType output_token_type_;
  const SymbolTable *input_symbols_;
  const SymbolTable *output_symbols_;
  PTree ptree_;
};

template <class StringType>
bool StringMapLineIsAcceptor(std::vector<StringType> line) {
  switch (line.size()) {
    case 1:
      return true;
    case 2:
    case 3: {
      return line[0] == line[1];
    }
    default:
      return false;
  }
}

template <class Weight>
bool StringMapLineIsAcceptor(
    const std::tuple<std::string, std::string, Weight> &line) {
  return std::get<0>(line) == std::get<1>(line);
}

inline bool StringMapSameTokenTypeKernel(TokenType input_token_type,
                                         TokenType output_token_type,
                                         const SymbolTable *input_symbols,
                                         const SymbolTable *output_symbols) {
  if (input_token_type != output_token_type) return false;
  switch (input_token_type) {
    case TokenType::BYTE:
    case TokenType::UTF8: {
      return true;
    }
    case TokenType::SYMBOL: {
      // The pointers should either both be nullptr or both be non-nullptr.
      if ((!input_symbols != !output_symbols)) return false;
      return CompatSymbols(input_symbols, output_symbols);
    }
  }
  return false;  // Unreachable.
}

inline bool StringMapCheckRepresentableAsAcceptor(
    internal::ColumnStringFile *csf, TokenType input_token_type,
    TokenType output_token_type, const SymbolTable *input_symbols,
    const SymbolTable *output_symbols) {
  if (!StringMapSameTokenTypeKernel(input_token_type, output_token_type,
                                    input_symbols, output_symbols)) {
    return false;
  }
  for (; !csf->Done(); csf->Next()) {
    const auto &line = csf->Row();
    if (!StringMapLineIsAcceptor(line)) return false;
  }
  return true;
}

template <class StringType>
bool StringMapCheckRepresentableAsAcceptor(std::vector<StringType> lines,
                                           TokenType input_token_type,
                                           TokenType output_token_type,
                                           const SymbolTable *input_symbols,
                                           const SymbolTable *output_symbols) {
  if (!StringMapSameTokenTypeKernel(input_token_type, output_token_type,
                                    input_symbols, output_symbols)) {
    return false;
  }
  for (const auto &line : lines) {
    if (!StringMapLineIsAcceptor(line)) return false;
  }
  return true;
}

template <class PTree, class Arc>
bool StringMapCompile(internal::ColumnStringFile *csf, MutableFst<Arc> *fst,
                      TokenType input_token_type, TokenType output_token_type,
                      const SymbolTable *input_symbols,
                      const SymbolTable *output_symbols) {
  internal::StringMapCompiler<Arc, PTree> compiler(
      input_token_type, output_token_type, input_symbols, output_symbols);
  for (csf->Reset(); !csf->Done(); csf->Next()) {
    const auto &line = csf->Row();
    const auto log_line_compilation_error = [&csf, &line]() {
      LOG(ERROR) << "StringFileCompile: Ill-formed line " << csf->LineNumber()
                 << " in file " << csf->Filename() << ": `"
                 << fst::StringJoin(line, "\t") << "`";
    };
    switch (line.size()) {
      case 1: {
        if (!compiler.Add(line[0])) {
          log_line_compilation_error();
          return false;
        }
        break;
      }
      case 2: {
        if (!compiler.Add(line[0], line[1])) {
          log_line_compilation_error();
          return false;
        }
        break;
      }
      case 3: {
        if (!compiler.Add(line[0], line[1], line[2])) {
          log_line_compilation_error();
          return false;
        }
        break;
      }
      default: {
        log_line_compilation_error();
        return false;
      }
    }
  }
  compiler.Compile(fst);
  return true;
}

template <class PTree, class Arc>
bool StringMapCompile(const std::vector<std::vector<std::string>> &lines,
                      MutableFst<Arc> *fst, TokenType input_token_type,
                      TokenType output_token_type,
                      const SymbolTable *input_symbols,
                      const SymbolTable *output_symbols) {
  internal::StringMapCompiler<Arc, PTree> compiler(
      input_token_type, output_token_type, input_symbols, output_symbols);
  for (const auto &line : lines) {
    const auto log_line_compilation_error = [&line]() {
      LOG(ERROR) << "StringMapCompile: Ill-formed line: `"
                 << fst::StringJoin(line, "\t") << "`";
    };
    switch (line.size()) {
      case 1: {
        if (!compiler.Add(line[0])) {
          log_line_compilation_error();
          return false;
        }
        break;
      }
      case 2: {
        if (!compiler.Add(line[0], line[1])) {
          log_line_compilation_error();
          return false;
        }
        break;
      }
      case 3: {
        if (!compiler.Add(line[0], line[1], line[2])) {
          log_line_compilation_error();
          return false;
        }
        break;
      }
      default: {
        log_line_compilation_error();
        return false;
      }
    }
  }
  compiler.Compile(fst);
  return true;
}

template <class PTree, class Arc>
bool StringMapCompile(
    const std::vector<
        std::tuple<std::string, std::string, typename Arc::Weight>> &lines,
    MutableFst<Arc> *fst, TokenType input_token_type = TokenType::BYTE,
    TokenType output_token_type = TokenType::BYTE,
    const SymbolTable *input_symbols = nullptr,
    const SymbolTable *output_symbols = nullptr) {
  internal::StringMapCompiler<Arc, PTree> compiler(
      input_token_type, output_token_type, input_symbols, output_symbols);
  for (const auto &line : lines) {
    const auto &istring = std::get<0>(line);
    const auto &ostring = std::get<1>(line);
    const auto &weight = std::get<2>(line);
    if (!compiler.Add(istring, ostring, weight)) {
      LOG(ERROR) << "StringMapCompile: Ill-formed line: `(" << istring << ", "
                 << ostring << ", " << weight << ")`";
      return false;
    }
  }
  compiler.Compile(fst);
  return true;
}

template <class Arc, class Container>
bool StringMapCompileWithAcceptorCheck(
    Container container, MutableFst<Arc> *fst,
    TokenType input_token_type = TokenType::BYTE,
    TokenType output_token_type = TokenType::BYTE,
    const SymbolTable *input_symbols = nullptr,
    const SymbolTable *output_symbols = nullptr) {
  const bool representable_as_acceptor =
      internal::StringMapCheckRepresentableAsAcceptor(
          container, input_token_type, output_token_type, input_symbols,
          output_symbols);
  if (representable_as_acceptor) {
    return internal::StringMapCompile<AcceptorPrefixTree<Arc>>(
        container, fst, input_token_type, output_token_type, input_symbols,
        output_symbols);
  } else {
    return internal::StringMapCompile<TransducerPrefixTree<Arc>>(
        container, fst, input_token_type, output_token_type, input_symbols,
        output_symbols);
  }
}

}  // namespace internal

// Compiles deterministic FST representing the union of the cross-product of
// pairs of weighted string cross-products from a TSV file of string triples.
// It will be an acceptor if all lines represent the same istring and ostring
// and also the (token_type, symbols) is the same for input and output.
template <class Arc>
bool StringFileCompile(
    const std::string &source, MutableFst<Arc> *fst,
    TokenType input_token_type = TokenType::BYTE,
    TokenType output_token_type = TokenType::BYTE,
    const SymbolTable *input_symbols = nullptr,
    const SymbolTable *output_symbols = nullptr) {
  internal::ColumnStringFile csf(source);
  if (csf.Error()) return false;  // File opening failed.
  return internal::StringMapCompileWithAcceptorCheck(
      &csf, fst, input_token_type, output_token_type, input_symbols,
      output_symbols);
}

// Compiles deterministic FST representing the union of the cross-product of
// pairs of weighted string cross-products from a vector of vector of strings.
// It will be an acceptor if all lines represent the same istring and ostring
// and also the (token_type, symbols) is the same for input and output.
template <class Arc>
bool StringMapCompile(
    const std::vector<std::vector<std::string>> &lines, MutableFst<Arc> *fst,
    TokenType input_token_type = TokenType::BYTE,
    TokenType output_token_type = TokenType::BYTE,
    const SymbolTable *input_symbols = nullptr,
    const SymbolTable *output_symbols = nullptr) {
  return internal::StringMapCompileWithAcceptorCheck(
      lines, fst, input_token_type, output_token_type, input_symbols,
      output_symbols);
}

// Compiles deterministic FST representing the union of the cross-product of
// pairs of weighted string cross-products from a vector of tuples of
// (istring, ostring, weight). It will be an acceptor if all lines represent the
// same istring and ostring and also the (token_type, symbols) is the same for
// input and output.
template <class Arc>
bool StringMapCompile(
    const std::vector<
        std::tuple<std::string, std::string, typename Arc::Weight>> &lines,
    MutableFst<Arc> *fst, TokenType input_token_type = TokenType::BYTE,
    TokenType output_token_type = TokenType::BYTE,
    const SymbolTable *input_symbols = nullptr,
    const SymbolTable *output_symbols = nullptr) {
  return internal::StringMapCompileWithAcceptorCheck(
      lines, fst, input_token_type, output_token_type, input_symbols,
      output_symbols);
}

}  // namespace fst

#endif  // PYNINI_STRINGMAP_H_

