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

#ifndef FST_SCRIPT_CONVERT_H_
#define FST_SCRIPT_CONVERT_H_

#include <memory>
#include <string>
#include <utility>

#include <fst/register.h>
#include <fst/script/arg-packs.h>
#include <fst/script/fst-class.h>

namespace fst {
namespace script {

using FstConvertInnerArgs = std::pair<const FstClass &, const std::string &>;

using FstConvertArgs =
    WithReturnValue<std::unique_ptr<FstClass>, FstConvertInnerArgs>;

template <class Arc>
void Convert(FstConvertArgs *args) {
  const Fst<Arc> &fst = *std::get<0>(args->args).GetFst<Arc>();
  const std::string &new_type = std::get<1>(args->args);
  std::unique_ptr<Fst<Arc>> result(Convert(fst, new_type));
  args->retval =
      result ? std::make_unique<FstClass>(std::move(result)) : nullptr;
}

std::unique_ptr<FstClass> Convert(const FstClass &fst,
                                  const std::string &new_type);

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_CONVERT_H_
