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

#include <fst/script/getters.h>

#include <string_view>

namespace fst {
namespace script {

bool GetArcFilterType(std::string_view str, ArcFilterType *arc_filter_type) {
  if (str == "any") {
    *arc_filter_type = ArcFilterType::ANY;
  } else if (str == "epsilon") {
    *arc_filter_type = ArcFilterType::EPSILON;
  } else if (str == "iepsilon") {
    *arc_filter_type = ArcFilterType::INPUT_EPSILON;
  } else if (str == "oepsilon") {
    *arc_filter_type = ArcFilterType::OUTPUT_EPSILON;
  } else {
    return false;
  }
  return true;
}

bool GetArcSortType(std::string_view str, ArcSortType *sort_type) {
  if (str == "ilabel") {
    *sort_type = ArcSortType::ILABEL;
  } else if (str == "olabel") {
    *sort_type = ArcSortType::OLABEL;
  } else {
    return false;
  }
  return true;
}

bool GetClosureType(std::string_view str, ClosureType *closure_type) {
  if (str == "star") {
    *closure_type = CLOSURE_STAR;
  } else if (str == "plus") {
    *closure_type = CLOSURE_PLUS;
  } else {
    return false;
  }
  return true;
}

bool GetComposeFilter(std::string_view str, ComposeFilter *compose_filter) {
  if (str == "alt_sequence") {
    *compose_filter = ALT_SEQUENCE_FILTER;
  } else if (str == "auto") {
    *compose_filter = AUTO_FILTER;
  } else if (str == "match") {
    *compose_filter = MATCH_FILTER;
  } else if (str == "no_match") {
    *compose_filter = NO_MATCH_FILTER;
  } else if (str == "null") {
    *compose_filter = NULL_FILTER;
  } else if (str == "sequence") {
    *compose_filter = SEQUENCE_FILTER;
  } else if (str == "trivial") {
    *compose_filter = TRIVIAL_FILTER;
  } else {
    return false;
  }
  return true;
}

bool GetDeterminizeType(std::string_view str, DeterminizeType *det_type) {
  if (str == "functional") {
    *det_type = DETERMINIZE_FUNCTIONAL;
  } else if (str == "nonfunctional") {
    *det_type = DETERMINIZE_NONFUNCTIONAL;
  } else if (str == "disambiguate") {
    *det_type = DETERMINIZE_DISAMBIGUATE;
  } else {
    return false;
  }
  return true;
}

bool GetEpsNormalizeType(std::string_view str,
                         EpsNormalizeType *eps_norm_type) {
  if (str == "input") {
    *eps_norm_type = EPS_NORM_INPUT;
  } else if (str == "output") {
    *eps_norm_type = EPS_NORM_OUTPUT;
  } else {
    return false;
  }
  return true;
}

bool GetMapType(std::string_view str, MapType *map_type) {
  if (str == "arc_sum") {
    *map_type = MapType::ARC_SUM;
  } else if (str == "arc_unique") {
    *map_type = MapType::ARC_UNIQUE;
  } else if (str == "identity") {
    *map_type = MapType::IDENTITY;
  } else if (str == "input_epsilon") {
    *map_type = MapType::INPUT_EPSILON;
  } else if (str == "invert") {
    *map_type = MapType::INVERT;
  } else if (str == "output_epsilon") {
    *map_type = MapType::OUTPUT_EPSILON;
  } else if (str == "plus") {
    *map_type = MapType::PLUS;
  } else if (str == "power") {
    *map_type = MapType::POWER;
  } else if (str == "quantize") {
    *map_type = MapType::QUANTIZE;
  } else if (str == "rmweight") {
    *map_type = MapType::RMWEIGHT;
  } else if (str == "superfinal") {
    *map_type = MapType::SUPERFINAL;
  } else if (str == "times") {
    *map_type = MapType::TIMES;
  } else if (str == "to_log") {
    *map_type = MapType::TO_LOG;
  } else if (str == "to_log64") {
    *map_type = MapType::TO_LOG64;
  } else if (str == "to_std" || str == "to_standard") {
    *map_type = MapType::TO_STD;
  } else {
    return false;
  }
  return true;
}

bool GetProjectType(std::string_view str, ProjectType *project_type) {
  if (str == "input") {
    *project_type = ProjectType::INPUT;
  } else if (str == "output") {
    *project_type = ProjectType::OUTPUT;
  } else {
    return false;
  }
  return true;
}

bool GetRandArcSelection(std::string_view str, RandArcSelection *ras) {
  if (str == "uniform") {
    *ras = RandArcSelection::UNIFORM;
  } else if (str == "log_prob") {
    *ras = RandArcSelection::LOG_PROB;
  } else if (str == "fast_log_prob") {
    *ras = RandArcSelection::FAST_LOG_PROB;
  } else {
    return false;
  }
  return true;
}

bool GetQueueType(std::string_view str, QueueType *queue_type) {
  if (str == "auto") {
    *queue_type = AUTO_QUEUE;
  } else if (str == "fifo") {
    *queue_type = FIFO_QUEUE;
  } else if (str == "lifo") {
    *queue_type = LIFO_QUEUE;
  } else if (str == "shortest") {
    *queue_type = SHORTEST_FIRST_QUEUE;
  } else if (str == "state") {
    *queue_type = STATE_ORDER_QUEUE;
  } else if (str == "top") {
    *queue_type = TOP_ORDER_QUEUE;
  } else {
    return false;
  }
  return true;
}

bool GetReplaceLabelType(std::string_view str, bool epsilon_on_replace,
                         ReplaceLabelType *rlt) {
  if (epsilon_on_replace || str == "neither") {
    *rlt = REPLACE_LABEL_NEITHER;
  } else if (str == "input") {
    *rlt = REPLACE_LABEL_INPUT;
  } else if (str == "output") {
    *rlt = REPLACE_LABEL_OUTPUT;
  } else if (str == "both") {
    *rlt = REPLACE_LABEL_BOTH;
  } else {
    return false;
  }
  return true;
}

bool GetReweightType(std::string_view str, ReweightType *reweight_type) {
  if (str == "to_initial") {
    *reweight_type = REWEIGHT_TO_INITIAL;
  } else if (str == "to_final") {
    *reweight_type = REWEIGHT_TO_FINAL;
  } else {
    return false;
  }
  return true;
}

bool GetTokenType(std::string_view str, TokenType *token_type) {
  if (str == "byte") {
    *token_type = TokenType::BYTE;
  } else if (str == "utf8") {
    *token_type = TokenType::UTF8;
  } else if (str == "symbol") {
    *token_type = TokenType::SYMBOL;
  } else {
    return false;
  }
  return true;
}

}  // namespace script
}  // namespace fst
