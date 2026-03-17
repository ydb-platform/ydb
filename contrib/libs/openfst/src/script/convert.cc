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

#include <fst/script/convert.h>

#include <string>
#include <utility>

#include <fst/script/script-impl.h>

namespace fst {
namespace script {

std::unique_ptr<FstClass> Convert(const FstClass &ifst,
                                  const std::string &new_type) {
  FstConvertInnerArgs iargs{ifst, new_type};
  FstConvertArgs args(iargs);
  Apply<Operation<FstConvertArgs>>("Convert", ifst.ArcType(), &args);
  return std::move(args.retval);
}

REGISTER_FST_OPERATION_3ARCS(Convert, FstConvertArgs);

}  // namespace script
}  // namespace fst
