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
// A generic (string,type) list file format.
//
// This is a stripped-down version of STTable that does not support the Find()
// operation but that does support reading/writting from standard in/out.

#ifndef FST_EXTENSIONS_FAR_STLIST_H_
#define FST_EXTENSIONS_FAR_STLIST_H_

#include <algorithm>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include <fstream>
#include <fst/util.h>
#include <string_view>

namespace fst {

inline constexpr int32_t kSTListMagicNumber = 5656924;
inline constexpr int32_t kSTListFileVersion = 1;

// String-type list writing class for object of type T using a functor Writer.
// The Writer functor must provide at least the following interface:
//
//   struct Writer {
//     void operator()(std::ostream &, const T &) const;
//   };
template <class T, class Writer>
class STListWriter {
 public:
  explicit STListWriter(const std::string &source)
      : stream_(source.empty()
                    ? &std::cout
                    : new std::ofstream(
                          source, std::ios_base::out | std::ios_base::binary)),
        error_(false) {
    WriteType(*stream_, kSTListMagicNumber);
    WriteType(*stream_, kSTListFileVersion);
    if (!stream_) {
      FSTERROR() << "STListWriter::STListWriter: Error writing to file: "
                 << source;
      error_ = true;
    }
  }

  static STListWriter<T, Writer> *Create(const std::string &source) {
    return new STListWriter<T, Writer>(source);
  }

  void Add(std::string_view key, const T &t) {
    if (key.empty()) {
      FSTERROR() << "STListWriter::Add: Key empty: " << key;
      error_ = true;
    } else if (key < last_key_) {
      FSTERROR() << "STListWriter::Add: Key out of order: " << key;
      error_ = true;
    }
    if (error_) return;
    // TODO(jrosenstock,glebm): Use assign(key) when C++17 is required
    last_key_.assign(key.data(), key.size());
    WriteType(*stream_, key);
    entry_writer_(*stream_, t);
  }

  bool Error() const { return error_; }

  ~STListWriter() {
    WriteType(*stream_, std::string());
    if (stream_ != &std::cout) delete stream_;
  }

 private:
  Writer entry_writer_;
  std::ostream *stream_;  // Output stream.
  std::string last_key_;  // Last key.
  bool error_;

  STListWriter(const STListWriter &) = delete;
  STListWriter &operator=(const STListWriter &) = delete;
};

// String-type list reading class for object of type T using a functor Reader.
// Reader must provide at least the following interface:
//
//   struct Reader {
//     T *operator()(std::istream &) const;
//   };
template <class T, class Reader>
class STListReader {
 public:
  explicit STListReader(const std::vector<std::string> &sources)
      : sources_(sources), error_(false) {
    streams_.resize(sources.size(), nullptr);
    bool has_stdin = false;
    for (size_t i = 0; i < sources.size(); ++i) {
      if (sources[i].empty()) {
        if (!has_stdin) {
          streams_[i] = &std::cin;
          sources_[i] = "stdin";
          has_stdin = true;
        } else {
          FSTERROR() << "STListReader::STListReader: Cannot read multiple "
                     << "inputs from standard input";
          error_ = true;
          return;
        }
      } else {
        streams_[i] = new std::ifstream(
            sources[i], std::ios_base::in | std::ios_base::binary);
        if (streams_[i]->fail()) {
          FSTERROR() << "STListReader::STListReader: Error reading file: "
                     << sources[i];
          error_ = true;
          return;
        }
      }
      int32_t magic_number = 0;
      ReadType(*streams_[i], &magic_number);
      int32_t file_version = 0;
      ReadType(*streams_[i], &file_version);
      if (magic_number != kSTListMagicNumber) {
        FSTERROR() << "STListReader::STListReader: Wrong file type: "
                   << sources[i];
        error_ = true;
        return;
      }
      if (file_version != kSTListFileVersion) {
        FSTERROR() << "STListReader::STListReader: Wrong file version: "
                   << sources[i];
        error_ = true;
        return;
      }
      std::string key;
      ReadType(*streams_[i], &key);
      if (!key.empty()) heap_.push(std::make_pair(key, i));
      if (!*streams_[i]) {
        FSTERROR() << "STListReader: Error reading file: " << sources_[i];
        error_ = true;
        return;
      }
    }
    if (heap_.empty()) return;
    const auto current = heap_.top().second;
    entry_.reset(entry_reader_(*streams_[current]));
    if (!entry_ || !*streams_[current]) {
      FSTERROR() << "STListReader: Error reading entry for key "
                 << heap_.top().first << ", file " << sources_[current];
      error_ = true;
    }
  }

  ~STListReader() {
    for (auto &stream : streams_) {
      if (stream != &std::cin) delete stream;
    }
  }

  static STListReader<T, Reader> *Open(const std::string &source) {
    std::vector<std::string> sources;
    sources.push_back(source);
    return new STListReader<T, Reader>(sources);
  }

  static STListReader<T, Reader> *Open(
      const std::vector<std::string> &sources) {
    return new STListReader<T, Reader>(sources);
  }

  void Reset() {
    FSTERROR() << "STListReader::Reset: Operation not supported";
    error_ = true;
  }

  bool Find([[maybe_unused]] std::string_view key) {
    FSTERROR() << "STListReader::Find: Operation not supported";
    error_ = true;
    return false;
  }

  bool Done() const { return error_ || heap_.empty(); }

  void Next() {
    if (error_) return;
    auto current = heap_.top().second;
    std::string key;
    heap_.pop();
    ReadType(*(streams_[current]), &key);
    if (!*streams_[current]) {
      FSTERROR() << "STListReader: Error reading file: " << sources_[current];
      error_ = true;
      return;
    }
    if (!key.empty()) heap_.push(std::make_pair(key, current));
    if (!heap_.empty()) {
      current = heap_.top().second;
      entry_.reset(entry_reader_(*streams_[current]));
      if (!entry_ || !*streams_[current]) {
        FSTERROR() << "STListReader: Error reading entry for key: "
                   << heap_.top().first << ", file: " << sources_[current];
        error_ = true;
      }
    }
  }

  const std::string &GetKey() const { return heap_.top().first; }

  const T *GetEntry() const { return entry_.get(); }

  bool Error() const { return error_; }

 private:
  Reader entry_reader_;                  // Read functor.
  std::vector<std::istream *> streams_;  // Input streams.
  std::vector<std::string> sources_;     // Corresponding sources.
  std::priority_queue<std::pair<std::string, size_t>,
                      std::vector<std::pair<std::string, size_t>>,
                      std::greater<std::pair<std::string, size_t>>>
      heap_;                          // (Key, stream id) heap
  mutable std::unique_ptr<T> entry_;  // The currently read entry.
  bool error_;

  STListReader(const STListReader &) = delete;
  STListReader &operator=(const STListReader &) = delete;
};

// String-type list header reading function, templated on the entry header type.
// The Header type must provide at least the following interface:
//
//  struct Header {
//    void Read(std::istream &strm, const string &source);
//  };
template <class Header>
bool ReadSTListHeader(const std::string &source, Header *header) {
  if (source.empty()) {
    LOG(ERROR) << "ReadSTListHeader: Can't read header from standard input";
    return false;
  }
  std::ifstream strm(source, std::ios_base::in | std::ios_base::binary);
  if (!strm) {
    LOG(ERROR) << "ReadSTListHeader: Could not open file: " << source;
    return false;
  }
  int32_t magic_number = 0;
  ReadType(strm, &magic_number);
  int32_t file_version = 0;
  ReadType(strm, &file_version);
  if (magic_number != kSTListMagicNumber) {
    LOG(ERROR) << "ReadSTListHeader: Wrong file type: " << source;
    return false;
  }
  if (file_version != kSTListFileVersion) {
    LOG(ERROR) << "ReadSTListHeader: Wrong file version: " << source;
    return false;
  }
  std::string key;
  ReadType(strm, &key);
  if (!strm) {
    LOG(ERROR) << "ReadSTListHeader: Error reading key: " << source;
    return false;
  }
  // Empty key is written last, so this is an empty STList.
  if (key.empty()) return true;
  if (!header->Read(strm, source + ":" + key)) {
    LOG(ERROR) << "ReadSTListHeader: Error reading FstHeader: " << source;
    return false;
  }
  if (!strm) {
    LOG(ERROR) << "ReadSTListHeader: Error reading file: " << source;
    return false;
  }
  return true;
}

bool IsSTList(const std::string &source);

}  // namespace fst

#endif  // FST_EXTENSIONS_FAR_STLIST_H_
