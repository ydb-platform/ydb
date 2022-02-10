//
// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// File: match.h
// -----------------------------------------------------------------------------
//
// This file contains simple utilities for performing string matching checks.
// All of these function parameters are specified as `y_absl::string_view`, 
// meaning that these functions can accept `TString`, `y_absl::string_view` or 
// NUL-terminated C-style strings.
//
// Examples:
//   TString s = "foo"; 
//   y_absl::string_view sv = "f"; 
//   assert(y_absl::StrContains(s, sv)); 
//
// Note: The order of parameters in these functions is designed to mimic the
// order an equivalent member function would exhibit;
// e.g. `s.Contains(x)` ==> `y_absl::StrContains(s, x). 
#ifndef ABSL_STRINGS_MATCH_H_
#define ABSL_STRINGS_MATCH_H_

#include <cstring>

#include "y_absl/strings/string_view.h" 

namespace y_absl { 
ABSL_NAMESPACE_BEGIN

// StrContains()
//
// Returns whether a given string `haystack` contains the substring `needle`.
inline bool StrContains(y_absl::string_view haystack,
                        y_absl::string_view needle) noexcept {
  return haystack.find(needle, 0) != haystack.npos;
}

inline bool StrContains(y_absl::string_view haystack, char needle) noexcept {
  return haystack.find(needle) != haystack.npos;
}

// StartsWith()
//
// Returns whether a given string `text` begins with `prefix`.
inline bool StartsWith(y_absl::string_view text,
                       y_absl::string_view prefix) noexcept {
  return prefix.empty() ||
         (text.size() >= prefix.size() &&
          memcmp(text.data(), prefix.data(), prefix.size()) == 0);
}

// EndsWith()
//
// Returns whether a given string `text` ends with `suffix`.
inline bool EndsWith(y_absl::string_view text,
                     y_absl::string_view suffix) noexcept {
  return suffix.empty() ||
         (text.size() >= suffix.size() &&
          memcmp(text.data() + (text.size() - suffix.size()), suffix.data(),
                 suffix.size()) == 0);
}

// EqualsIgnoreCase()
//
// Returns whether given ASCII strings `piece1` and `piece2` are equal, ignoring
// case in the comparison.
bool EqualsIgnoreCase(y_absl::string_view piece1,
                      y_absl::string_view piece2) noexcept;

// StartsWithIgnoreCase()
//
// Returns whether a given ASCII string `text` starts with `prefix`,
// ignoring case in the comparison.
bool StartsWithIgnoreCase(y_absl::string_view text,
                          y_absl::string_view prefix) noexcept;

// EndsWithIgnoreCase()
//
// Returns whether a given ASCII string `text` ends with `suffix`, ignoring
// case in the comparison.
bool EndsWithIgnoreCase(y_absl::string_view text,
                        y_absl::string_view suffix) noexcept;

ABSL_NAMESPACE_END
}  // namespace y_absl 

#endif  // ABSL_STRINGS_MATCH_H_
