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
// Map helper for FAR processing.

#ifndef FST_EXTENSIONS_FAR_MAP_REDUCE_H_
#define FST_EXTENSIONS_FAR_MAP_REDUCE_H_

#include <fst/extensions/far/far.h>
#include <fst/arc.h>
#include <fst/fst.h>
#include <string_view>

namespace fst {
namespace internal {

// This function applies a functor to each FST in a FAR reader, writing the
// resulting FST to a FAR writer. The functor must support the following
// interface:
//
// std::unique_ptr<Fst<Arc>> operator()(std::string_view key,
//                                      const Fst<Arc> *fst);
//
// The functor signals an error by returning a null pointer.
//
// One can imagine more expressive variants of this: e.g., one which also
// transforms the key, but this can be added as needed.
//
// The caller is responsible for rewinding the reader afterwards, if desired,
// and for checking the error bit of the reader and writer.
template <class Arc, class Functor>
void Map(FarReader<Arc> &reader, FarWriter<Arc> &writer, Functor functor) {
  for (; !reader.Done(); reader.Next()) {
    const auto &key = reader.GetKey();
    const auto fst = functor(key, reader.GetFst());
    if (!fst) return;
    writer.Add(key, *fst);
  }
}

// This function applies a boolean functor to pairs of FSTs passed via FAR
// readers, returning false if any of the keys do not match or if any of the
// functor calls are false. The functor must support the following interface:
//
// bool operator()(std::string_view key, const Fst<Arc> *fst1,
//                 const Fst<Arc> *fst2);
//
// One can imagine more expressive variants of this, but these can be added as
// needed.
//
// The caller is responsible for rewinding the readers afterwards, if desired,
// and for checking the error bits of the readers.
template <class Arc, class Functor>
bool MapAllReduce(FarReader<Arc> &reader1, FarReader<Arc> &reader2,
                  Functor functor, std::string_view begin_key = "",
                  std::string_view end_key = "") {
  if (!begin_key.empty()) {
    const bool find_begin1 = reader1.Find(begin_key);
    const bool find_begin2 = reader2.Find(begin_key);
    if (!find_begin1 || !find_begin2) {
      const bool ret = !find_begin1 && !find_begin2;
      if (!ret) {
        LOG(ERROR) << "MapAllReduce: Key " << begin_key << " missing from "
                   << (find_begin1 ? "second" : "first") << " FAR";
      }
      return ret;
    }
  }
  for (; !reader1.Done() && !reader2.Done(); reader1.Next(), reader2.Next()) {
    const auto &key1 = reader1.GetKey();
    const auto &key2 = reader2.GetKey();
    if (!end_key.empty() && end_key < key1 && end_key < key2) {
      return true;
    }
    if (key1 != key2) {
      LOG(ERROR) << "MapAllReduce: Mismatched keys " << key1 << " and " << key2;
      return false;
    }
    if (!functor(key1, reader1.GetFst(), reader2.GetFst())) return false;
  }
  if (reader1.Done() && !reader2.Done()) {
    LOG(ERROR) << "MapAllReduce: Key " << reader2.GetKey()
               << " missing from first FAR";
    return false;
  } else if (reader2.Done() && !reader1.Done()) {
    LOG(ERROR) << "MapAllReduce: Key " << reader1.GetKey()
               << " missing from second FAR";
    return false;
  }
  return true;
}

}  // namespace internal
}  // namespace fst

#endif  // FST_EXTENSIONS_FAR_MAP_REDUCE_H_
