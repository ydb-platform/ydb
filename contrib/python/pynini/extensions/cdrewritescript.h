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


#ifndef PYNINI_CDREWRITESCRIPT_H_
#define PYNINI_CDREWRITESCRIPT_H_

#include <cstdint>
#include <tuple>
#include <utility>

#include <fst/compat.h>
#include <fst/script/fst-class.h>
#include "cdrewrite.h"

namespace fst {
namespace script {

using FstCDRewriteCompileArgs =
    std::tuple<const FstClass &, const FstClass &, const FstClass &,
               const FstClass &, MutableFstClass *, CDRewriteDirection,
               CDRewriteMode, int64_t, int64_t>;

template <class Arc>
void CDRewriteCompile(FstCDRewriteCompileArgs *args) {
  const Fst<Arc> &tau = *(std::get<0>(*args).GetFst<Arc>());
  const Fst<Arc> &lambda = *(std::get<1>(*args).GetFst<Arc>());
  const Fst<Arc> &rho = *(std::get<2>(*args).GetFst<Arc>());
  const Fst<Arc> &sigma = *(std::get<3>(*args).GetFst<Arc>());
  MutableFst<Arc> *ofst = std::get<4>(*args)->GetMutableFst<Arc>();
  const CDRewriteDirection dir = std::get<5>(*args);
  const CDRewriteMode mode = std::get<6>(*args);
  const typename Arc::Label initial_boundary_marker = std::get<7>(*args);
  const typename Arc::Label final_boundary_marker = std::get<8>(*args);
  CDRewriteCompile(tau, lambda, rho, sigma, ofst, dir, mode,
                   initial_boundary_marker, final_boundary_marker);
}

void CDRewriteCompile(const FstClass &tau, const FstClass &lambda,
                      const FstClass &rho, const FstClass &sigma,
                      MutableFstClass *ofst, CDRewriteDirection dir,
                      CDRewriteMode mode,
                      int64_t initial_boundary_marker = kNoLabel,
                      int64_t final_boundary_marker = kNoLabel);

}  // namespace script
}  // namespace fst

#endif  // PYNINI_CDREWRITESCRIPT_H_

