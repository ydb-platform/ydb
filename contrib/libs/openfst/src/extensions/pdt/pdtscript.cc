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
// Definitions of 'scriptable' versions of pdt operations, that is,
// those that can be called with FstClass-type arguments.
//
// See comments in nlp/fst/script/script-impl.h for how the registration
// mechanism allows these to work with various arc types.

#include <fst/extensions/pdt/pdtscript.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <fst/extensions/pdt/compose.h>
#include <fst/extensions/pdt/expand.h>
#include <fst/extensions/pdt/replace.h>
#include <fst/extensions/pdt/reverse.h>
#include <fst/extensions/pdt/shortest-path.h>
#include <fst/script/script-impl.h>

namespace fst {
namespace script {

void Compose(const FstClass &ifst1, const FstClass &ifst2,
             const std::vector<std::pair<int64_t, int64_t>> &parens,
             MutableFstClass *ofst, const PdtComposeOptions &copts,
             bool left_pdt) {
  if (!internal::ArcTypesMatch(ifst1, ifst2, "Compose") ||
      !internal::ArcTypesMatch(ifst1, *ofst, "Compose")) {
    return;
  }
  PdtComposeArgs args{ifst1, ifst2, parens, ofst, copts, left_pdt};
  Apply<Operation<PdtComposeArgs>>("Compose", ifst1.ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(Compose, PdtComposeArgs);

void Expand(const FstClass &ifst,
            const std::vector<std::pair<int64_t, int64_t>> &parens,
            MutableFstClass *ofst, const PdtExpandOptions &opts) {
  PdtExpandArgs args{ifst, parens, ofst, opts};
  Apply<Operation<PdtExpandArgs>>("Expand", ifst.ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(Expand, PdtExpandArgs);

void Expand(const FstClass &ifst,
            const std::vector<std::pair<int64_t, int64_t>> &parens,
            MutableFstClass *ofst, bool connect, bool keep_parentheses,
            const WeightClass &weight_threshold) {
  Expand(ifst, parens, ofst,
         PdtExpandOptions(connect, keep_parentheses, weight_threshold));
}

void Replace(const std::vector<std::pair<int64_t, const FstClass *>> &pairs,
             MutableFstClass *ofst,
             std::vector<std::pair<int64_t, int64_t>> *parens, int64_t root,
             PdtParserType parser_type, int64_t start_paren_labels,
             const std::string &left_paren_prefix,
             const std::string &right_paren_prefix) {
  for (size_t i = 1; i < pairs.size(); ++i) {
    if (!internal::ArcTypesMatch(*pairs[i - 1].second, *pairs[i].second,
                                 "Replace"))
      return;
  }
  if (!internal::ArcTypesMatch(*pairs[0].second, *ofst, "PdtReplace")) return;
  PdtReplaceArgs args{pairs,
                      ofst,
                      parens,
                      root,
                      parser_type,
                      start_paren_labels,
                      left_paren_prefix,
                      right_paren_prefix};
  Apply<Operation<PdtReplaceArgs>>("Replace", ofst->ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(Replace, PdtReplaceArgs);

void Reverse(const FstClass &ifst,
             const std::vector<std::pair<int64_t, int64_t>> &parens,
             MutableFstClass *ofst) {
  PdtReverseArgs args{ifst, parens, ofst};
  Apply<Operation<PdtReverseArgs>>("Reverse", ifst.ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(Reverse, PdtReverseArgs);

void ShortestPath(const FstClass &ifst,
                  const std::vector<std::pair<int64_t, int64_t>> &parens,
                  MutableFstClass *ofst, const PdtShortestPathOptions &opts) {
  PdtShortestPathArgs args{ifst, parens, ofst, opts};
  Apply<Operation<PdtShortestPathArgs>>("ShortestPath", ifst.ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(ShortestPath, PdtShortestPathArgs);

void Info(const FstClass &ifst,
          const std::vector<std::pair<int64_t, int64_t>> &parens) {
  PdtInfoArgs args(ifst, parens);
  Apply<Operation<PdtInfoArgs>>("Info", ifst.ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(Info, PdtInfoArgs);

}  // namespace script
}  // namespace fst
