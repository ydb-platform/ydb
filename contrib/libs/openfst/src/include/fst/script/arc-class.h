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

#ifndef FST_SCRIPT_ARC_CLASS_H_
#define FST_SCRIPT_ARC_CLASS_H_

#include <cstdint>

#include <fst/script/weight-class.h>

namespace fst {
namespace script {

// A struct representing an arc while ignoring arc type. It is passed as an
// argument to AddArc.

struct ArcClass {
  template <class Arc>
  explicit ArcClass(const Arc &arc)
      : ilabel(arc.ilabel),
        olabel(arc.olabel),
        weight(arc.weight),
        nextstate(arc.nextstate) {}

  ArcClass(int64_t ilabel, int64_t olabel, const WeightClass &weight,
           int64_t nextstate)
      : ilabel(ilabel), olabel(olabel), weight(weight), nextstate(nextstate) {}

  template <class Arc>
  Arc GetArc() const {
    return Arc(ilabel, olabel, *(weight.GetWeight<typename Arc::Weight>()),
               nextstate);
  }

  int64_t ilabel;
  int64_t olabel;
  WeightClass weight;
  int64_t nextstate;
};

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_ARC_CLASS_H_
