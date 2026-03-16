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


#ifndef PYNINI_CHECKPROPS_H_
#define PYNINI_CHECKPROPS_H_

#include <string>

#include <fst/fst.h>

namespace fst {
namespace internal {

// Checks that an FST is an unweighted acceptor.
template <class Arc>
bool CheckUnweightedAcceptor(const Fst<Arc> &fst, const std::string &op_name,
                             const std::string &fst_name) {
  static constexpr auto props = kAcceptor | kUnweighted;
  if (fst.Properties(props, true) != props) {
    LOG(ERROR) << op_name << ": " << fst_name
               << " must be a unweighted acceptor";
    return false;
  }
  return true;
}

}  // namespace internal
}  // namespace fst

#endif  // PYNINI_CHECKPROPS_H_

