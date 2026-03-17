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
// Getters for converting command-line arguments into the appropriate enums
// or bitmasks, with the simplest ones defined as inline.

#ifndef FST_SCRIPT_GETTERS_H_
#define FST_SCRIPT_GETTERS_H_

#include <cstdint>
#include <string>

#include <fst/log.h>
#include <fst/compose.h>            // For ComposeFilter.
#include <fst/determinize.h>        // For DeterminizeType.
#include <fst/encode.h>             // For kEncodeLabels (etc.).
#include <fst/epsnormalize.h>       // For EpsNormalizeType.
#include <fst/project.h>            // For ProjectType.
#include <fst/push.h>               // For kPushWeights (etc.).
#include <fst/queue.h>              // For QueueType.
#include <fst/rational.h>           // For ClosureType.
#include <fst/string.h>             // For TokenType.
#include <fst/script/arcfilter-impl.h>  // For ArcFilterType.
#include <fst/script/arcsort.h>         // For ArcSortType.
#include <fst/script/map.h>             // For MapType.
#include <fst/script/script-impl.h>     // For RandArcSelection.
#include <string_view>

namespace fst {
namespace script {

bool GetArcFilterType(std::string_view str, ArcFilterType *arc_filter_type);

bool GetArcSortType(std::string_view str, ArcSortType *sort_type);

bool GetClosureType(std::string_view str, ClosureType *closure_type);

bool GetComposeFilter(std::string_view str, ComposeFilter *compose_filter);

bool GetDeterminizeType(std::string_view str, DeterminizeType *det_type);

inline uint8_t GetEncodeFlags(bool encode_labels, bool encode_weights) {
  return (encode_labels ? kEncodeLabels : 0) |
         (encode_weights ? kEncodeWeights : 0);
}

bool GetEpsNormalizeType(std::string_view str,
                         EpsNormalizeType *eps_norm_type);

bool GetMapType(std::string_view str, MapType *map_type);

bool GetProjectType(std::string_view str, ProjectType *project_type);

inline uint8_t GetPushFlags(bool push_weights, bool push_labels,
                            bool remove_total_weight,
                            bool remove_common_affix) {
  return ((push_weights ? kPushWeights : 0) | (push_labels ? kPushLabels : 0) |
          (remove_total_weight ? kPushRemoveTotalWeight : 0) |
          (remove_common_affix ? kPushRemoveCommonAffix : 0));
}

bool GetQueueType(std::string_view str, QueueType *queue_type);

bool GetRandArcSelection(std::string_view str, RandArcSelection *ras);

bool GetReplaceLabelType(std::string_view str, bool epsilon_on_replace,
                         ReplaceLabelType *rlt);

bool GetReweightType(std::string_view str, ReweightType *reweight_type);

bool GetTokenType(std::string_view str, TokenType *token_type);

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_GETTERS_H_
