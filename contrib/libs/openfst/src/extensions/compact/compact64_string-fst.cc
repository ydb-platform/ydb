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

#include <cstdint>

#include <fst/compact-fst.h>
#include <fst/fst.h>

namespace fst {

static FstRegisterer<CompactStringFst<StdArc, uint64_t>>
    CompactStringFst_StdArc_uint64_registerer;

static FstRegisterer<CompactStringFst<LogArc, uint64_t>>
    CompactStringFst_LogArc_uint64_registerer;

static FstRegisterer<CompactStringFst<Log64Arc, uint64_t>>
    CompactStringFst_Log64Arc_uint64_registerer;

}  // namespace fst
