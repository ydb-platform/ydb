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


#include "stringutil.h"

#include <string>

#include <fst/compat.h>
#include <string_view>

namespace fst {
namespace {

void StringReplace(std::string *full, const std::string &before,
                   const std::string &after) {
  size_t pos = 0;
  while ((pos = full->find(before, pos)) != std::string::npos) {
    full->replace(pos, before.size(), after);
    pos += after.size();
  }
}

std::string_view StripComment(std::string_view line) {
  char prev_char = '\0';
  for (size_t i = 0; i < line.size(); ++i) {
    const char this_char = line[i];
    if (this_char == '#' && prev_char != '\\') {
      // Strips comment and any trailing whitespace.
      return fst::StripTrailingAsciiWhitespace(line.substr(0, i));
    }
    prev_char = this_char;
  }
  return line;
}

}  // namespace

std::string StripCommentAndRemoveEscape(std::string_view line) {
  std::string stripped(StripComment(line));
  StringReplace(&stripped, "\\#", "#");
  return stripped;
}

std::string Escape(std::string_view str) {
  std::string result;
  result.reserve(str.size());
  for (char ch : str) {
    switch (ch) {
      case '[':
      case ']':
      case '\\':
        result.push_back('\\');
    }
    result.push_back(ch);
  }
  return result;
}

}  // namespace fst

