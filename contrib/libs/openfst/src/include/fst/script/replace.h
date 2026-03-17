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

#ifndef FST_SCRIPT_REPLACE_H_
#define FST_SCRIPT_REPLACE_H_

#include <cstdint>
#include <tuple>
#include <utility>
#include <vector>

#include <fst/replace.h>
#include <fst/script/fst-class.h>

namespace fst {
namespace script {

struct ReplaceOptions {
  const int64_t root;                        // Root rule for expansion.
  const ReplaceLabelType call_label_type;    // How to label call arc.
  const ReplaceLabelType return_label_type;  // How to label return arc.
  const int64_t return_label;                // Specifies return arc label.

  explicit ReplaceOptions(
      int64_t root, ReplaceLabelType call_label_type = REPLACE_LABEL_INPUT,
      ReplaceLabelType return_label_type = REPLACE_LABEL_NEITHER,
      int64_t return_label = 0)
      : root(root),
        call_label_type(call_label_type),
        return_label_type(return_label_type),
        return_label(return_label) {}
};

using FstReplaceArgs =
    std::tuple<const std::vector<std::pair<int64_t, const FstClass *>> &,
               MutableFstClass *, const ReplaceOptions &>;

template <class Arc>
void Replace(FstReplaceArgs *args) {
  // Now that we know the arc type, we construct a vector of
  // std::pair<real label, real fst> that the real Replace will use.
  const auto &untyped_pairs = std::get<0>(*args);
  std::vector<std::pair<typename Arc::Label, const Fst<Arc> *>> typed_pairs;
  typed_pairs.reserve(untyped_pairs.size());
  for (const auto &untyped_pair : untyped_pairs) {
    typed_pairs.emplace_back(untyped_pair.first,  // Converts label.
                             untyped_pair.second->GetFst<Arc>());
  }
  MutableFst<Arc> *ofst = std::get<1>(*args)->GetMutableFst<Arc>();
  const auto &opts = std::get<2>(*args);
  ReplaceFstOptions<Arc> typed_opts(opts.root, opts.call_label_type,
                                    opts.return_label_type, opts.return_label);
  ReplaceFst<Arc> rfst(typed_pairs, typed_opts);
  // Checks for cyclic dependencies before attempting expansion.
  if (rfst.CyclicDependencies()) {
    FSTERROR() << "Replace: Cyclic dependencies detected; cannot expand";
    ofst->SetProperties(kError, kError);
    return;
  }
  typed_opts.gc = true;  // Caching options to speed up batch copy.
  typed_opts.gc_limit = 0;
  *ofst = rfst;
}

void Replace(const std::vector<std::pair<int64_t, const FstClass *>> &pairs,
             MutableFstClass *ofst, const ReplaceOptions &opts);

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_REPLACE_H_
