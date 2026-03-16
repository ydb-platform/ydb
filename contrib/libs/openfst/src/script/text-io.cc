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

#include <fst/script/text-io.h>

#include <cstring>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>

#include <fst/log.h>
#include <fstream>
#include <fst/util.h>
#include <fst/windows_defs.inc>
#include <string_view>

namespace fst {
namespace script {

// Reads vector of weights; returns true on success.
bool ReadPotentials(std::string_view weight_type, const std::string &source,
                    std::vector<WeightClass> *potentials) {
  std::ifstream istrm(source);
  if (!istrm) {
    LOG(ERROR) << "ReadPotentials: Can't open file: " << source;
    return false;
  }
  static constexpr int kLineLen = 8096;
  char line[kLineLen];
  size_t nline = 0;
  potentials->clear();
  while (!istrm.getline(line, kLineLen).fail()) {
    ++nline;
    std::vector<std::string_view> col =
        StrSplit(line, ByAnyChar("\n\t "), SkipEmpty());
    if (col.empty() || col[0].empty()) continue;
    if (col.size() != 2) {
      FSTERROR() << "ReadPotentials: Bad number of columns, "
                 << "file = " << source << ", line = " << nline;
      return false;
    }
    const ssize_t s = StrToInt64(col[0], source, nline, false);
    const WeightClass weight(weight_type, col[1]);
    while (potentials->size() <= s) {
      potentials->push_back(WeightClass::Zero(weight_type));
    }
    potentials->back() = weight;
  }
  return true;
}

// Writes vector of weights; returns true on success.
bool WritePotentials(const std::string &source,
                     const std::vector<WeightClass> &potentials) {
  std::ofstream ostrm;
  if (!source.empty()) {
    ostrm.open(source);
    if (!ostrm) {
      LOG(ERROR) << "WritePotentials: Can't open file: " << source;
      return false;
    }
  }
  std::ostream &strm = ostrm.is_open() ? ostrm : std::cout;
  strm.precision(9);
  for (size_t s = 0; s < potentials.size(); ++s) {
    strm << s << "\t" << potentials[s] << "\n";
  }
  if (strm.fail()) {
    LOG(ERROR) << "WritePotentials: Write failed: "
               << (source.empty() ? "standard output" : source);
    return false;
  }
  return true;
}

}  // namespace script
}  // namespace fst
