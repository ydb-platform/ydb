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

#ifndef FST_SCRIPT_ARCFILTER_IMPL_H_
#define FST_SCRIPT_ARCFILTER_IMPL_H_

#include <cstdint>

namespace fst::script {

enum class ArcFilterType : uint8_t {
  ANY,
  EPSILON,
  INPUT_EPSILON,
  OUTPUT_EPSILON
};

}  // namespace fst::script

#endif  // FST_SCRIPT_ARCFILTER_IMPL_H_
