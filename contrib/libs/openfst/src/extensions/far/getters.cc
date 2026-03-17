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
//
// Definitions and functions for invoking and using Far main functions that
// support multiple and extensible arc types.

#include <fst/extensions/far/getters.h>

#include <cstdint>
#include <string>
#include <vector>

#include <fstream>

namespace fst {

namespace script {

bool GetFarType(std::string_view str, FarType *far_type) {
  if (str == "fst") {
    *far_type = FarType::FST;
  } else if (str == "stlist") {
    *far_type = FarType::STLIST;
  } else if (str == "sttable") {
    *far_type = FarType::STTABLE;
  } else if (str == "default") {
    *far_type = FarType::DEFAULT;
  } else {
    return false;
  }
  return true;
}

bool GetFarEntryType(std::string_view str, FarEntryType *entry_type) {
  if (str == "line") {
    *entry_type = FarEntryType::LINE;
  } else if (str == "file") {
    *entry_type = FarEntryType::FILE;
  } else {
    return false;
  }
  return true;
}

void ExpandArgs(int argc, char **argv, int *argcp, char ***argvp) {
}

}  // namespace script

std::string GetFarTypeString(FarType far_type) {
  switch (far_type) {
    case FarType::FST:
      return "fst";
    case FarType::STLIST:
      return "stlist";
    case FarType::STTABLE:
      return "sttable";
    case FarType::DEFAULT:
      return "default";
    default:
      return "<unknown>";
  }
}

}  // namespace fst
