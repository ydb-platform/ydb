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
// Creates a finite-state archive from component FSTs.

#ifndef FST_EXTENSIONS_FAR_CREATE_H_
#define FST_EXTENSIONS_FAR_CREATE_H_

#include <libgen.h>

#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

#include <fst/extensions/far/far.h>

namespace fst {

template <class Arc>
void Create(const std::vector<std::string> &sources, FarWriter<Arc> &writer,
            int32_t generate_keys, const std::string &key_prefix,
            const std::string &key_suffix) {
  for (size_t i = 0; i < sources.size(); ++i) {
    std::unique_ptr<Fst<Arc>> ifst(Fst<Arc>::Read(sources[i]));
    if (!ifst) return;
    std::string key;
    if (generate_keys > 0) {
      std::ostringstream keybuf;
      keybuf.width(generate_keys);
      keybuf.fill('0');
      keybuf << i + 1;
      key = keybuf.str();
    } else {
      auto source =
          fst::make_unique_for_overwrite<char[]>(sources[i].size() + 1);
      strcpy(source.get(), sources[i].c_str());  // NOLINT(runtime/printf)
      key = basename(source.get());
    }
    writer.Add(key_prefix + key + key_suffix, *ifst);
  }
}

}  // namespace fst

#endif  // FST_EXTENSIONS_FAR_CREATE_H_
