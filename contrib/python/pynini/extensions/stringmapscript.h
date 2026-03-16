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


#ifndef PYNINI_STRINGMAPSCRIPT_H_
#define PYNINI_STRINGMAPSCRIPT_H_

#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <fst/fstlib.h>
#include <fst/script/arg-packs.h>
#include <fst/script/fstscript.h>
#include <fst/script/weight-class.h>
#include "stringmap.h"

namespace fst {
namespace script {

using FstStringFileCompileInnerArgs =
    std::tuple<const std::string &, MutableFstClass *, TokenType, TokenType,
               const SymbolTable *, const SymbolTable *>;

using FstStringFileCompileArgs =
    WithReturnValue<bool, FstStringFileCompileInnerArgs>;

template <class Arc>
void StringFileCompile(FstStringFileCompileArgs *args) {
  MutableFst<Arc> *fst = std::get<1>(args->args)->GetMutableFst<Arc>();
  args->retval =
      StringFileCompile(std::get<0>(args->args), fst, std::get<2>(args->args),
                        std::get<3>(args->args), std::get<4>(args->args),
                        std::get<5>(args->args));
}

bool StringFileCompile(const std::string &source, MutableFstClass *fst,
                       TokenType input_token_type = TokenType::BYTE,
                       TokenType output_token_type = TokenType::BYTE,
                       const SymbolTable *input_symbols = nullptr,
                       const SymbolTable *output_symbols = nullptr);

using FstStringMapCompileInnerArgs1 =
    std::tuple<const std::vector<std::vector<std::string>> &, MutableFstClass *,
               TokenType, TokenType, const SymbolTable *, const SymbolTable *>;

using FstStringMapCompileArgs1 =
    WithReturnValue<bool, FstStringMapCompileInnerArgs1>;

template <class Arc>
void StringMapCompile(FstStringMapCompileArgs1 *args) {
  MutableFst<Arc> *fst = std::get<1>(args->args)->GetMutableFst<Arc>();
  args->retval =
      StringMapCompile(std::get<0>(args->args), fst, std::get<2>(args->args),
                       std::get<3>(args->args), std::get<4>(args->args),
                       std::get<5>(args->args));
}

using FstStringMapCompileInnerArgs2 = std::tuple<
    const std::vector<std::tuple<std::string, std::string, WeightClass>> &,
    MutableFstClass *, TokenType, TokenType, const SymbolTable *,
    const SymbolTable *>;

using FstStringMapCompileArgs2 =
    WithReturnValue<bool, FstStringMapCompileInnerArgs2>;

template <class Arc>
void StringMapCompile(FstStringMapCompileArgs2 *args) {
  std::vector<std::tuple<std::string, std::string, typename Arc::Weight>> lines;
  for (const auto &line : std::get<0>(args->args)) {
    const auto &istring = std::get<0>(line);
    const auto &ostring = std::get<1>(line);
    const auto &weight = *std::get<2>(line).GetWeight<typename Arc::Weight>();
    // NOTE: For correctness, we *could* verify that the weight of every one of
    // these arcs matches the weight used by fst, but this isn't strictly
    // necessary.
    lines.emplace_back(istring, ostring, weight);
  }
  MutableFst<Arc> *fst = std::get<1>(args->args)->GetMutableFst<Arc>();
  args->retval = StringMapCompile(
      lines, fst, std::get<2>(args->args), std::get<3>(args->args),
      std::get<4>(args->args), std::get<5>(args->args));
}

bool StringMapCompile(const std::vector<std::vector<std::string>> &lines,
                      MutableFstClass *fst,
                      TokenType input_token_type = TokenType::BYTE,
                      TokenType output_token_type = TokenType::BYTE,
                      const SymbolTable *input_symbols = nullptr,
                      const SymbolTable *output_symbols = nullptr);

bool StringMapCompile(
    const std::vector<std::tuple<std::string, std::string, WeightClass>> &lines,
    MutableFstClass *fst, TokenType input_token_type = TokenType::BYTE,
    TokenType output_token_type = TokenType::BYTE,
    const SymbolTable *input_symbols = nullptr,
    const SymbolTable *output_symbols = nullptr);

}  // namespace script
}  // namespace fst

#endif  // PYNINI_STRINGMAPSCRIPT_H_

