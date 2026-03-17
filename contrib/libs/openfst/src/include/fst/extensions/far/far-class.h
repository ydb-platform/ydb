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
// Scripting API support for FarReader and FarWriter.

#ifndef FST_EXTENSIONS_FAR_FAR_CLASS_H_
#define FST_EXTENSIONS_FAR_FAR_CLASS_H_

#include <memory>
#include <string>
#include <vector>

#include <fst/extensions/far/far.h>
#include <fst/script/arg-packs.h>
#include <fst/script/fstscript.h>

namespace fst {
namespace script {

// FarReader API.

// Virtual interface implemented by each concrete FarReaderImpl<A>.
// See the FarReader interface in far.h for the exact semantics.
class FarReaderImplBase {
 public:
  virtual const std::string &ArcType() const = 0;
  virtual bool Done() const = 0;
  virtual bool Error() const = 0;
  virtual const std::string &GetKey() const = 0;
  virtual const FstClass *GetFstClass() const = 0;
  virtual bool Find(const std::string &key) = 0;
  virtual void Next() = 0;
  virtual void Reset() = 0;
  virtual FarType Type() const = 0;
  virtual ~FarReaderImplBase() {}
};

// Templated implementation.
template <class Arc>
class FarReaderClassImpl : public FarReaderImplBase {
 public:
  explicit FarReaderClassImpl(const std::string &source)
      : reader_(FarReader<Arc>::Open(source)) {}

  explicit FarReaderClassImpl(const std::vector<std::string> &sources)
      : reader_(FarReader<Arc>::Open(sources)) {}

  const std::string &ArcType() const final { return Arc::Type(); }

  bool Done() const final { return reader_->Done(); }

  bool Error() const final { return reader_->Error(); }

  bool Find(const std::string &key) final { return reader_->Find(key); }

  const FstClass *GetFstClass() const final {
    fstc_ = std::make_unique<FstClass>(*reader_->GetFst());
    return fstc_.get();
  }

  const std::string &GetKey() const final { return reader_->GetKey(); }

  void Next() final { return reader_->Next(); }

  void Reset() final { reader_->Reset(); }

  FarType Type() const final { return reader_->Type(); }

  const FarReader<Arc> *GetFarReader() const { return reader_.get(); }

  FarReader<Arc> *GetFarReader() { return reader_.get(); }

 private:
  std::unique_ptr<FarReader<Arc>> reader_;
  mutable std::unique_ptr<FstClass> fstc_;
};

class FarReaderClass;

using OpenFarReaderClassArgs =
    WithReturnValue<std::unique_ptr<FarReaderClass>,
                    const std::vector<std::string> &>;

// Untemplated user-facing class holding a templated pimpl.
class FarReaderClass {
 public:
  const std::string &ArcType() const { return impl_->ArcType(); }

  bool Done() const { return impl_->Done(); }

  // Returns True if the impl is null (i.e., due to read failure).
  // Attempting to call any other function will result in null dereference.
  bool Error() const { return (impl_) ? impl_->Error() : true; }

  bool Find(const std::string &key) { return impl_->Find(key); }

  const FstClass *GetFstClass() const { return impl_->GetFstClass(); }

  const std::string &GetKey() const { return impl_->GetKey(); }

  void Next() { impl_->Next(); }

  void Reset() { impl_->Reset(); }

  FarType Type() const { return impl_->Type(); }

  template <class Arc>
  const FarReader<Arc> *GetFarReader() const {
    if (Arc::Type() != ArcType()) return nullptr;
    const FarReaderClassImpl<Arc> *typed_impl =
        down_cast<FarReaderClassImpl<Arc> *>(impl_.get());
    return typed_impl->GetFarReader();
  }

  template <class Arc>
  FarReader<Arc> *GetFarReader() {
    if (Arc::Type() != ArcType()) return nullptr;
    FarReaderClassImpl<Arc> *typed_impl =
        down_cast<FarReaderClassImpl<Arc> *>(impl_.get());
    return typed_impl->GetFarReader();
  }

  template <class Arc>
  friend void OpenFarReaderClass(OpenFarReaderClassArgs *args);

  // Defined in the CC.

  static std::unique_ptr<FarReaderClass> Open(const std::string &source);

  static std::unique_ptr<FarReaderClass> Open(
      const std::vector<std::string> &sources);

 private:
  template <class Arc>
  explicit FarReaderClass(std::unique_ptr<FarReaderClassImpl<Arc>> impl)
      : impl_(std::move(impl)) {}

  std::unique_ptr<FarReaderImplBase> impl_;
};

// These exist solely for registration purposes; users should call the
// static method FarReaderClass::Open instead.

template <class Arc>
void OpenFarReaderClass(OpenFarReaderClassArgs *args) {
  auto impl = std::make_unique<FarReaderClassImpl<Arc>>(args->args);
  if (impl->GetFarReader() == nullptr) {
    // Underlying reader failed to open, so return failure here, too.
    args->retval = nullptr;
  } else {
    args->retval = fst::WrapUnique(new FarReaderClass(std::move(impl)));
  }
}

// FarWriter API.

// Virtual interface implemented by each concrete FarWriterImpl<A>.
class FarWriterImplBase {
 public:
  // Unlike the lower-level library, this returns a boolean to signal failure
  // due to non-conformant arc types.
  virtual bool Add(const std::string &key, const FstClass &fst) = 0;
  virtual const std::string &ArcType() const = 0;
  virtual bool Error() const = 0;
  virtual FarType Type() const = 0;
  virtual ~FarWriterImplBase() {}
};

// Templated implementation.
template <class Arc>
class FarWriterClassImpl : public FarWriterImplBase {
 public:
  explicit FarWriterClassImpl(const std::string &source,
                              FarType type = FarType::DEFAULT)
      : writer_(FarWriter<Arc>::Create(source, type)) {}

  bool Add(const std::string &key, const FstClass &fst) final {
    if (ArcType() != fst.ArcType()) {
      FSTERROR() << "Cannot write FST with " << fst.ArcType() << " arcs to "
                 << "FAR with " << ArcType() << " arcs";
      return false;
    }
    writer_->Add(key, *(fst.GetFst<Arc>()));
    return true;
  }

  const std::string &ArcType() const final { return Arc::Type(); }

  bool Error() const final { return writer_->Error(); }

  FarType Type() const final { return writer_->Type(); }

  const FarWriter<Arc> *GetFarWriter() const { return writer_.get(); }

  FarWriter<Arc> *GetFarWriter() { return writer_.get(); }

 private:
  std::unique_ptr<FarWriter<Arc>> writer_;
};

class FarWriterClass;

using CreateFarWriterClassInnerArgs = std::pair<const std::string &, FarType>;

using CreateFarWriterClassArgs =
    WithReturnValue<std::unique_ptr<FarWriterClass>,
                    CreateFarWriterClassInnerArgs>;

// Untemplated user-facing class holding a templated pimpl.
class FarWriterClass {
 public:
  static std::unique_ptr<FarWriterClass> Create(
      const std::string &source, const std::string &arc_type,
      FarType type = FarType::DEFAULT);

  bool Add(const std::string &key, const FstClass &fst) {
    return impl_->Add(key, fst);
  }

  // Returns True if the impl is null (i.e., due to construction failure).
  // Attempting to call any other function will result in null dereference.
  bool Error() const { return (impl_) ? impl_->Error() : true; }

  const std::string &ArcType() const { return impl_->ArcType(); }

  FarType Type() const { return impl_->Type(); }

  template <class Arc>
  const FarWriter<Arc> *GetFarWriter() const {
    if (Arc::Type() != ArcType()) return nullptr;
    const FarWriterClassImpl<Arc> *typed_impl =
        down_cast<FarWriterClassImpl<Arc> *>(impl_.get());
    return typed_impl->GetFarWriter();
  }

  template <class Arc>
  FarWriter<Arc> *GetFarWriter() {
    if (Arc::Type() != ArcType()) return nullptr;
    FarWriterClassImpl<Arc> *typed_impl =
        down_cast<FarWriterClassImpl<Arc> *>(impl_.get());
    return typed_impl->GetFarWriter();
  }

  template <class Arc>
  friend void CreateFarWriterClass(CreateFarWriterClassArgs *args);

 private:
  template <class Arc>
  explicit FarWriterClass(std::unique_ptr<FarWriterClassImpl<Arc>> impl)
      : impl_(std::move(impl)) {}

  std::unique_ptr<FarWriterImplBase> impl_;
};

// This exists solely for registration purposes; users should call the
// static method FarWriterClass::Create instead.
template <class Arc>
void CreateFarWriterClass(CreateFarWriterClassArgs *args) {
  args->retval = fst::WrapUnique(
      new FarWriterClass(std::make_unique<FarWriterClassImpl<Arc>>(
          std::get<0>(args->args), std::get<1>(args->args))));
}

}  // namespace script
}  // namespace fst

#endif  // FST_EXTENSIONS_FAR_FAR_CLASS_H_
