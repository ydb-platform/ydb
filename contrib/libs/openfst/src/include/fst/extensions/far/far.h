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
// Finite-State Transducer (FST) archive classes.

#ifndef FST_EXTENSIONS_FAR_FAR_H_
#define FST_EXTENSIONS_FAR_FAR_H_

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>

#include <fst/log.h>
#include <fst/extensions/far/stlist.h>
#include <fst/extensions/far/sttable.h>
#include <fst/arc.h>
#include <fstream>
#include <fst/fst.h>
#include <fst/vector-fst.h>
#include <string_view>

namespace fst {

enum class FarEntryType { LINE, FILE };

enum class FarType {
  DEFAULT = 0,
  STTABLE = 1,
  STLIST = 2,
  FST = 3,
};

// Checks for FST magic number in an input stream (to be opened given the source
// name), to indicate to the caller function that the stream content is an FST
// header.
inline bool IsFst(const std::string &source) {
  std::ifstream strm(source, std::ios_base::in | std::ios_base::binary);
  if (!strm) return false;
  int32_t magic_number = 0;
  ReadType(strm, &magic_number);
  bool match = magic_number == kFstMagicNumber;
  return match;
}

// FST archive header class
class FarHeader {
 public:
  const std::string &ArcType() const { return arctype_; }

  enum FarType FarType() const { return fartype_; }

  bool Read(const std::string &source) {
    FstHeader fsthdr;
    arctype_ = "unknown";
    if (source.empty()) {
      // Header reading unsupported on stdin. Assumes STList and StdArc.
      fartype_ = FarType::STLIST;
      arctype_ = "standard";
      return true;
    } else if (IsSTTable(source)) {  // Checks if STTable.
      fartype_ = FarType::STTABLE;
      if (!ReadSTTableHeader(source, &fsthdr)) return false;
      arctype_ = fsthdr.ArcType().empty() ? ErrorArc::Type() : fsthdr.ArcType();
      return true;
    } else if (IsSTList(source)) {  // Checks if STList.
      fartype_ = FarType::STLIST;
      if (!ReadSTListHeader(source, &fsthdr)) return false;
      arctype_ = fsthdr.ArcType().empty() ? ErrorArc::Type() : fsthdr.ArcType();
      return true;
    } else if (IsFst(source)) {  // Checks if FST.
      fartype_ = FarType::FST;
      std::ifstream istrm(source,
                               std::ios_base::in | std::ios_base::binary);
      if (!fsthdr.Read(istrm, source)) return false;
      arctype_ = fsthdr.ArcType().empty() ? ErrorArc::Type() : fsthdr.ArcType();
      return true;
    }
    return false;
  }

 private:
  enum FarType fartype_;
  std::string arctype_;
};

// This class creates an archive of FSTs.
template <class A>
class FarWriter {
 public:
  using Arc = A;

  // Creates a new (empty) FST archive; returns null on error.
  static FarWriter *Create(const std::string &source,
                           FarType type = FarType::DEFAULT);

  // Adds an FST to the end of an archive. Keys must be non-empty and
  // in lexicographic order. FSTs must have a suitable write method.
  virtual void Add(std::string_view key, const Fst<Arc> &fst) = 0;

  virtual FarType Type() const = 0;

  virtual bool Error() const = 0;

  virtual ~FarWriter() {}

 protected:
  FarWriter() {}
};

// This class iterates through an existing archive of FSTs.
template <class A>
class FarReader {
 public:
  using Arc = A;

  // Opens an existing FST archive in a single file; returns null on error.
  // Sets current position to the beginning of the achive.
  static FarReader *Open(const std::string &source);

  // Opens an existing FST archive in multiple files; returns null on error.
  // Sets current position to the beginning of the achive.
  static FarReader *Open(const std::vector<std::string> &sources);

  // Resets current position to beginning of archive.
  virtual void Reset() = 0;

  // Sets current position to first entry >= key. Returns true if a match.
  virtual bool Find(std::string_view key) = 0;

  // Current position at end of archive?
  virtual bool Done() const = 0;

  // Move current position to next FST.
  virtual void Next() = 0;

  // Returns key at the current position. This reference is invalidated if
  // the current position in the archive is changed.
  virtual const std::string &GetKey() const = 0;

  // Returns pointer to FST at the current position. This is invalidated if
  // the current position in the archive is changed.
  virtual const Fst<Arc> *GetFst() const = 0;

  virtual FarType Type() const = 0;

  virtual bool Error() const = 0;

  virtual ~FarReader() {}

 protected:
  FarReader() {}
};

template <class Arc>
class FstWriter {
 public:
  void operator()(std::ostream &strm, const Fst<Arc> &fst) const {
    fst.Write(strm, FstWriteOptions());
  }
};

template <class A>
class STTableFarWriter : public FarWriter<A> {
 public:
  using Arc = A;

  static STTableFarWriter *Create(const std::string &source) {
    auto *writer = STTableWriter<Fst<Arc>, FstWriter<Arc>>::Create(source);
    return new STTableFarWriter(writer);
  }

  void Add(std::string_view key, const Fst<Arc> &fst) final {
    writer_->Add(key, fst);
  }

  FarType Type() const final { return FarType::STTABLE; }

  bool Error() const final { return writer_->Error(); }

 private:
  explicit STTableFarWriter(STTableWriter<Fst<Arc>, FstWriter<Arc>> *writer)
      : writer_(writer) {}

  std::unique_ptr<STTableWriter<Fst<Arc>, FstWriter<Arc>>> writer_;
};

template <class A>
class STListFarWriter : public FarWriter<A> {
 public:
  using Arc = A;

  static STListFarWriter *Create(const std::string &source) {
    auto *writer = STListWriter<Fst<Arc>, FstWriter<Arc>>::Create(source);
    return new STListFarWriter(writer);
  }

  void Add(std::string_view key, const Fst<Arc> &fst) final {
    writer_->Add(key, fst);
  }

  FarType Type() const final { return FarType::STLIST; }

  bool Error() const final { return writer_->Error(); }

 private:
  explicit STListFarWriter(STListWriter<Fst<Arc>, FstWriter<Arc>> *writer)
      : writer_(writer) {}

  std::unique_ptr<STListWriter<Fst<Arc>, FstWriter<Arc>>> writer_;
};

template <class A>
class FstFarWriter final : public FarWriter<A> {
 public:
  using Arc = A;

  explicit FstFarWriter(const std::string &source)
      : source_(source), error_(false), written_(false) {}

  static FstFarWriter *Create(const std::string &source) {
    return new FstFarWriter(source);
  }

  void Add([[maybe_unused]] std::string_view key, const Fst<A> &fst) final {
    if (written_) {
      LOG(WARNING) << "FstFarWriter::Add: only one FST supported,"
                   << " subsequent entries discarded.";
    } else {
      error_ = !fst.Write(source_);
      written_ = true;
    }
  }

  FarType Type() const final { return FarType::FST; }

  bool Error() const final { return error_; }

  ~FstFarWriter() final {}

 private:
  std::string source_;
  bool error_;
  bool written_;
};

template <class Arc>
FarWriter<Arc> *FarWriter<Arc>::Create(const std::string &source,
                                       FarType type) {
  switch (type) {
    case FarType::DEFAULT:
      if (source.empty()) return STListFarWriter<Arc>::Create(source);
    case FarType::STTABLE:
      return STTableFarWriter<Arc>::Create(source);
    case FarType::STLIST:
      return STListFarWriter<Arc>::Create(source);
    case FarType::FST:
      return FstFarWriter<Arc>::Create(source);
    default:
      LOG(ERROR) << "FarWriter::Create: Unknown FAR type";
      return nullptr;
  }
}

template <class Arc>
class FstReader {
 public:
  Fst<Arc> *operator()(std::istream &strm,
                       const FstReadOptions &options = FstReadOptions()) const {
    return Fst<Arc>::Read(strm, options);
  }
};

template <class A>
class STTableFarReader : public FarReader<A> {
 public:
  using Arc = A;

  static STTableFarReader *Open(const std::string &source) {
    auto reader =
        fst::WrapUnique(STTableReader<Fst<Arc>, FstReader<Arc>>::Open(source));
    if (!reader || reader->Error()) return nullptr;
    return new STTableFarReader(std::move(reader));
  }

  static STTableFarReader *Open(const std::vector<std::string> &sources) {
    auto reader = fst::WrapUnique(
        STTableReader<Fst<Arc>, FstReader<Arc>>::Open(sources));
    if (!reader || reader->Error()) return nullptr;
    return new STTableFarReader(std::move(reader));
  }

  void Reset() final { reader_->Reset(); }

  bool Find(std::string_view key) final { return reader_->Find(key); }

  bool Done() const final { return reader_->Done(); }

  void Next() final { return reader_->Next(); }

  const std::string &GetKey() const final { return reader_->GetKey(); }

  const Fst<Arc> *GetFst() const final { return reader_->GetEntry(); }

  FarType Type() const final { return FarType::STTABLE; }

  bool Error() const final { return reader_->Error(); }

 private:
  explicit STTableFarReader(
      std::unique_ptr<STTableReader<Fst<Arc>, FstReader<Arc>>> reader)
      : reader_(std::move(reader)) {}

  std::unique_ptr<STTableReader<Fst<Arc>, FstReader<Arc>>> reader_;
};

template <class A>
class STListFarReader : public FarReader<A> {
 public:
  using Arc = A;

  static STListFarReader *Open(const std::string &source) {
    auto reader =
        fst::WrapUnique(STListReader<Fst<Arc>, FstReader<Arc>>::Open(source));
    if (!reader || reader->Error()) return nullptr;
    return new STListFarReader(std::move(reader));
  }

  static STListFarReader *Open(const std::vector<std::string> &sources) {
    auto reader =
        fst::WrapUnique(STListReader<Fst<Arc>, FstReader<Arc>>::Open(sources));
    if (!reader || reader->Error()) return nullptr;
    return new STListFarReader(std::move(reader));
  }

  void Reset() final { reader_->Reset(); }

  bool Find(std::string_view key) final { return reader_->Find(key); }

  bool Done() const final { return reader_->Done(); }

  void Next() final { return reader_->Next(); }

  const std::string &GetKey() const final { return reader_->GetKey(); }

  const Fst<Arc> *GetFst() const final { return reader_->GetEntry(); }

  FarType Type() const final { return FarType::STLIST; }

  bool Error() const final { return reader_->Error(); }

 private:
  explicit STListFarReader(
      std::unique_ptr<STListReader<Fst<Arc>, FstReader<Arc>>> reader)
      : reader_(std::move(reader)) {}

  std::unique_ptr<STListReader<Fst<Arc>, FstReader<Arc>>> reader_;
};

template <class A>
class FstFarReader final : public FarReader<A> {
 public:
  using Arc = A;

  static FstFarReader *Open(const std::string &source) {
    std::vector<std::string> sources;
    sources.push_back(source);
    return new FstFarReader<Arc>(sources);
  }

  static FstFarReader *Open(const std::vector<std::string> &sources) {
    return new FstFarReader<Arc>(sources);
  }

  explicit FstFarReader(const std::vector<std::string> &sources)
      : keys_(sources), has_stdin_(false), pos_(0), error_(false) {
    std::sort(keys_.begin(), keys_.end());
    streams_.resize(keys_.size(), nullptr);
    for (size_t i = 0; i < keys_.size(); ++i) {
      if (keys_[i].empty()) {
        if (!has_stdin_) {
          streams_[i] = &std::cin;
          has_stdin_ = true;
        } else {
          FSTERROR() << "FstFarReader::FstFarReader: standard input should "
                        "only appear once in the input file list";
          error_ = true;
          return;
        }
      } else {
        streams_[i] = new std::ifstream(
            keys_[i], std::ios_base::in | std::ios_base::binary);
        if (streams_[i]->fail()) {
          FSTERROR() << "FstFarReader::FstFarReader: Error reading file: "
                     << sources[i];
          error_ = true;
          return;
        }
      }
    }
    if (pos_ >= keys_.size()) return;
    ReadFst();
  }

  void Reset() final {
    if (has_stdin_) {
      FSTERROR()
          << "FstFarReader::Reset: Operation not supported on standard input";
      error_ = true;
      return;
    }
    pos_ = 0;
    ReadFst();
  }

  bool Find([[maybe_unused]] std::string_view key) final {
    if (has_stdin_) {
      FSTERROR()
          << "FstFarReader::Find: Operation not supported on standard input";
      error_ = true;
      return false;
    }
    pos_ = 0;  // TODO
    ReadFst();
    return true;
  }

  bool Done() const final { return error_ || pos_ >= keys_.size(); }

  void Next() final {
    ++pos_;
    ReadFst();
  }

  const std::string &GetKey() const final { return keys_[pos_]; }

  const Fst<Arc> *GetFst() const final { return fst_.get(); }

  FarType Type() const final { return FarType::FST; }

  bool Error() const final { return error_; }

  ~FstFarReader() final {
    for (size_t i = 0; i < keys_.size(); ++i) {
      if (streams_[i] != &std::cin) {
        delete streams_[i];
      }
    }
  }

 private:
  void ReadFst() {
    fst_.reset();
    if (pos_ >= keys_.size()) return;
    streams_[pos_]->seekg(0);
    fst_.reset(Fst<Arc>::Read(*streams_[pos_], FstReadOptions()));
    if (!fst_) {
      FSTERROR() << "FstFarReader: Error reading Fst from: " << keys_[pos_];
      error_ = true;
    }
  }

  std::vector<std::string> keys_;
  std::vector<std::istream *> streams_;
  bool has_stdin_;
  size_t pos_;
  mutable std::unique_ptr<Fst<Arc>> fst_;
  mutable bool error_;
};

template <class Arc>
FarReader<Arc> *FarReader<Arc>::Open(const std::string &source) {
  if (source.empty())
    return STListFarReader<Arc>::Open(source);
  else if (IsSTTable(source))
    return STTableFarReader<Arc>::Open(source);
  else if (IsSTList(source))
    return STListFarReader<Arc>::Open(source);
  else if (IsFst(source))
    return FstFarReader<Arc>::Open(source);
  return nullptr;
}

template <class Arc>
FarReader<Arc> *FarReader<Arc>::Open(const std::vector<std::string> &sources) {
  if (!sources.empty() && sources[0].empty())
    return STListFarReader<Arc>::Open(sources);
  else if (!sources.empty() && IsSTTable(sources[0]))
    return STTableFarReader<Arc>::Open(sources);
  else if (!sources.empty() && IsSTList(sources[0]))
    return STListFarReader<Arc>::Open(sources);
  else if (!sources.empty() && IsFst(sources[0]))
    return FstFarReader<Arc>::Open(sources);
  return nullptr;
}

}  // namespace fst

#endif  // FST_EXTENSIONS_FAR_FAR_H_
