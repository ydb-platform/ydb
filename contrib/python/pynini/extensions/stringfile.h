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


#ifndef PYNINI_STRINGFILE_H_
#define PYNINI_STRINGFILE_H_

#include <string>
#include <vector>

#include <fstream>
#include <fst/compat.h>
#include <string_view>

namespace fst {
namespace internal {

// Basic line-by-line file iterator, with support for line numbers and
// \# comment stripping.
class StringFile {
 public:
  // Opens a file input stream using the provided filename.
  explicit StringFile(const std::string &source)
      : istrm_(source), linenum_(0), source_(source) {
    Next();
  }

  void Reset();

  void Next();

  bool Done() const { return !istrm_; }

  const std::string &GetString() const { return line_; }

  size_t LineNumber() const { return linenum_; }

  const std::string &Filename() const { return source_; }

  bool Error() const { return !istrm_.is_open() || istrm_.bad(); }

 private:
  std::ifstream istrm_;
  std::string line_;
  size_t linenum_;
  const std::string source_;
};

// File iterator expecting multiple columns separated by tab.
class ColumnStringFile {
 public:
  explicit ColumnStringFile(const std::string &source) : sf_(source) {
    Parse();
  }

  void Reset();

  void Next();

  bool Done() const { return sf_.Done(); }

  // Access to the underlying row vector.
  const std::vector<std::string_view> &Row() const { return row_; }

  size_t LineNumber() const { return sf_.LineNumber(); }

  const std::string &Filename() const { return sf_.Filename(); }

  bool Error() const { return sf_.Error(); }

 private:
  void Parse() { row_ = fst::StrSplit(sf_.GetString(), '\t'); }

  StringFile sf_;
  std::vector<std::string_view> row_;
};

}  // namespace internal
}  // namespace fst

#endif  // PYNINI_STRINGFILE_H_

