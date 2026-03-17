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


#include "stringfile.h"

#include "stringutil.h"

namespace fst {
namespace internal {

void StringFile::Reset() {
  istrm_.clear();
  istrm_.seekg(0, istrm_.beg);
  Next();
}

// Tries to read a non-empty line until EOF.
void StringFile::Next() {
  do {
    ++linenum_;
    if (!std::getline(istrm_, line_)) return;
    line_ = StripCommentAndRemoveEscape(line_);
  } while (line_.empty());
}

void ColumnStringFile::Reset() {
  sf_.Reset();
  Parse();
}

void ColumnStringFile::Next() {
  sf_.Next();
  Parse();
}

}  // namespace internal
}  // namespace fst

