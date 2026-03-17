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
// Definitions of 'scriptable' versions of mpdt operations, that is,
// those that can be called with FstClass-type arguments.
//
// See comments in nlp/fst/script/script-impl.h for how the registration
// mechanism allows these to work with various arc types.

#include <fst/extensions/mpdt/mpdtscript.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <fst/extensions/mpdt/compose.h>
#include <fst/extensions/mpdt/expand.h>
#include <fst/extensions/mpdt/reverse.h>
#include <fst/script/script-impl.h>

namespace fst {
namespace script {

void Compose(const FstClass &ifst1, const FstClass &ifst2,
             const std::vector<std::pair<int64_t, int64_t>> &parens,
             const std::vector<int64_t> &assignments, MutableFstClass *ofst,
             const MPdtComposeOptions &copts, bool left_pdt) {
  if (!internal::ArcTypesMatch(ifst1, ifst2, "Compose") ||
      !internal::ArcTypesMatch(ifst1, *ofst, "Compose"))
    return;
  MPdtComposeArgs args{ifst1, ifst2, parens,  assignments,
                       ofst,  copts, left_pdt};
  Apply<Operation<MPdtComposeArgs>>("Compose", ifst1.ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(Compose, MPdtComposeArgs);

void Expand(const FstClass &ifst,
            const std::vector<std::pair<int64_t, int64_t>> &parens,
            const std::vector<int64_t> &assignments, MutableFstClass *ofst,
            const MPdtExpandOptions &opts) {
  MPdtExpandArgs args{ifst, parens, assignments, ofst, opts};
  Apply<Operation<MPdtExpandArgs>>("Expand", ifst.ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(Expand, MPdtExpandArgs);

void Expand(const FstClass &ifst,
            const std::vector<std::pair<int64_t, int64_t>> &parens,
            const std::vector<int64_t> &assignments, MutableFstClass *ofst,
            bool connect) {
  Expand(ifst, parens, assignments, ofst, MPdtExpandOptions(connect));
}

void Reverse(const FstClass &ifst,
             const std::vector<std::pair<int64_t, int64_t>> &parens,
             std::vector<int64_t> *assignments, MutableFstClass *ofst) {
  MPdtReverseArgs args{ifst, parens, assignments, ofst};
  Apply<Operation<MPdtReverseArgs>>("Reverse", ifst.ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(Reverse, MPdtReverseArgs);

void Info(const FstClass &ifst,
          const std::vector<std::pair<int64_t, int64_t>> &parens,
          const std::vector<int64_t> &assignments) {
  MPdtInfoArgs args{ifst, parens, assignments};
  Apply<Operation<MPdtInfoArgs>>("Info", ifst.ArcType(), &args);
}

REGISTER_FST_OPERATION_3ARCS(Info, MPdtInfoArgs);

}  // namespace script
}  // namespace fst
