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

#ifndef FST_SCRIPT_ENCODEMAPPER_CLASS_H_
#define FST_SCRIPT_ENCODEMAPPER_CLASS_H_

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include <fst/encode.h>
#include <fst/generic-register.h>
#include <fst/script/arc-class.h>
#include <fst/script/fst-class.h>
#include <string_view>

// Scripting API support for EncodeMapper.

namespace fst {
namespace script {

// Virtual interface implemented by each concrete EncodeMapperClassImpl<Arc>.
class EncodeMapperImplBase {
 public:
  // Returns an encoded ArcClass.
  virtual ArcClass operator()(const ArcClass &) = 0;
  virtual const std::string &ArcType() const = 0;
  virtual const std::string &WeightType() const = 0;
  virtual EncodeMapperImplBase *Copy() const = 0;
  virtual uint8_t Flags() const = 0;
  virtual uint64_t Properties(uint64_t) = 0;
  virtual EncodeType Type() const = 0;
  virtual bool Write(const std::string &) const = 0;
  virtual bool Write(std::ostream &, const std::string &) const = 0;
  virtual const SymbolTable *InputSymbols() const = 0;
  virtual const SymbolTable *OutputSymbols() const = 0;
  virtual void SetInputSymbols(const SymbolTable *) = 0;
  virtual void SetOutputSymbols(const SymbolTable *) = 0;
  virtual ~EncodeMapperImplBase() {}
};

// Templated implementation.
template <class Arc>
class EncodeMapperClassImpl : public EncodeMapperImplBase {
 public:
  explicit EncodeMapperClassImpl(const EncodeMapper<Arc> &mapper)
      : mapper_(mapper) {}

  ArcClass operator()(const ArcClass &a) final;

  const std::string &ArcType() const final { return Arc::Type(); }

  const std::string &WeightType() const final { return Arc::Weight::Type(); }

  EncodeMapperClassImpl<Arc> *Copy() const final {
    return new EncodeMapperClassImpl<Arc>(mapper_);
  }

  uint8_t Flags() const final { return mapper_.Flags(); }

  uint64_t Properties(uint64_t inprops) final {
    return mapper_.Properties(inprops);
  }

  EncodeType Type() const final { return mapper_.Type(); }

  bool Write(const std::string &source) const final {
    return mapper_.Write(source);
  }

  bool Write(std::ostream &strm, const std::string &source) const final {
    return mapper_.Write(strm, source);
  }

  const SymbolTable *InputSymbols() const final {
    return mapper_.InputSymbols();
  }

  const SymbolTable *OutputSymbols() const final {
    return mapper_.OutputSymbols();
  }

  void SetInputSymbols(const SymbolTable *syms) final {
    mapper_.SetInputSymbols(syms);
  }

  void SetOutputSymbols(const SymbolTable *syms) final {
    mapper_.SetOutputSymbols(syms);
  }

  ~EncodeMapperClassImpl() override {}

  const EncodeMapper<Arc> *GetImpl() const { return &mapper_; }

  EncodeMapper<Arc> *GetImpl() { return &mapper_; }

 private:
  EncodeMapper<Arc> mapper_;
};

template <class Arc>
inline ArcClass EncodeMapperClassImpl<Arc>::operator()(const ArcClass &a) {
  const Arc arc(a.ilabel, a.olabel,
                *(a.weight.GetWeight<typename Arc::Weight>()), a.nextstate);
  return ArcClass(mapper_(arc));
}

class EncodeMapperClass {
 public:
  EncodeMapperClass() : impl_(nullptr) {}

  EncodeMapperClass(std::string_view arc_type, uint8_t flags,
                    EncodeType type = ENCODE);

  template <class Arc>
  explicit EncodeMapperClass(const EncodeMapper<Arc> &mapper)
      : impl_(std::make_unique<EncodeMapperClassImpl<Arc>>(mapper)) {}

  EncodeMapperClass(const EncodeMapperClass &other)
      : impl_(other.impl_ == nullptr ? nullptr : other.impl_->Copy()) {}

  EncodeMapperClass &operator=(const EncodeMapperClass &other) {
    impl_.reset(other.impl_ == nullptr ? nullptr : other.impl_->Copy());
    return *this;
  }

  ArcClass operator()(const ArcClass &arc) { return (*impl_)(arc); }

  const std::string &ArcType() const { return impl_->ArcType(); }

  const std::string &WeightType() const { return impl_->WeightType(); }

  uint8_t Flags() const { return impl_->Flags(); }

  uint64_t Properties(uint64_t inprops) { return impl_->Properties(inprops); }

  EncodeType Type() const { return impl_->Type(); }

  static std::unique_ptr<EncodeMapperClass> Read(const std::string &source);

  static std::unique_ptr<EncodeMapperClass> Read(std::istream &strm,
                                                 const std::string &source);

  bool Write(const std::string &source) const { return impl_->Write(source); }

  bool Write(std::ostream &strm, const std::string &source) const {
    return impl_->Write(strm, source);
  }

  const SymbolTable *InputSymbols() const { return impl_->InputSymbols(); }

  const SymbolTable *OutputSymbols() const { return impl_->OutputSymbols(); }

  void SetInputSymbols(const SymbolTable *syms) {
    impl_->SetInputSymbols(syms);
  }

  void SetOutputSymbols(const SymbolTable *syms) {
    impl_->SetOutputSymbols(syms);
  }

  // Implementation stuff.

  template <class Arc>
  EncodeMapper<Arc> *GetEncodeMapper() {
    if (Arc::Type() != ArcType()) {
      return nullptr;
    } else {
      auto *typed_impl = down_cast<EncodeMapperClassImpl<Arc> *>(impl_.get());
      return typed_impl->GetImpl();
    }
  }

  template <class Arc>
  const EncodeMapper<Arc> *GetEncodeMapper() const {
    if (Arc::Type() != ArcType()) {
      return nullptr;
    } else {
      auto *typed_impl = down_cast<EncodeMapperClassImpl<Arc> *>(impl_.get());
      return typed_impl->GetImpl();
    }
  }

  // Required for registration.

  template <class Arc>
  static std::unique_ptr<EncodeMapperClass> Read(std::istream &strm,
                                                 const std::string &source) {
    std::unique_ptr<EncodeMapper<Arc>> mapper(
        EncodeMapper<Arc>::Read(strm, source));
    return mapper ? std::make_unique<EncodeMapperClass>(*mapper) : nullptr;
  }

  template <class Arc>
  static std::unique_ptr<EncodeMapperImplBase> Create(
      uint8_t flags, EncodeType type = ENCODE) {
    return std::make_unique<EncodeMapperClassImpl<Arc>>(
        EncodeMapper<Arc>(flags, type));
  }

 private:
  explicit EncodeMapperClass(std::unique_ptr<EncodeMapperImplBase> impl)
      : impl_(std::move(impl)) {}

  const EncodeMapperImplBase *GetImpl() const { return impl_.get(); }

  EncodeMapperImplBase *GetImpl() { return impl_.get(); }

  std::unique_ptr<EncodeMapperImplBase> impl_;
};

// Registration for EncodeMapper types.

// This class definition is to avoid a nested class definition inside the
// EncodeMapperIORegistration struct.

template <class Reader, class Creator>
struct EncodeMapperClassRegEntry {
  Reader reader;
  Creator creator;

  EncodeMapperClassRegEntry(Reader reader, Creator creator)
      : reader(reader), creator(creator) {}

  EncodeMapperClassRegEntry() : reader(nullptr), creator(nullptr) {}
};

template <class Reader, class Creator>
class EncodeMapperClassIORegister
    : public GenericRegister<std::string,
                             EncodeMapperClassRegEntry<Reader, Creator>,
                             EncodeMapperClassIORegister<Reader, Creator>> {
 public:
  Reader GetReader(std::string_view arc_type) const {
    return this->GetEntry(arc_type).reader;
  }

  Creator GetCreator(std::string_view arc_type) const {
    return this->GetEntry(arc_type).creator;
  }

 protected:
  std::string ConvertKeyToSoFilename(std::string_view key) const final {
    std::string legal_type(key);
    ConvertToLegalCSymbol(&legal_type);
    legal_type.append("-arc.so");
    return legal_type;
  }
};

// Struct containing everything needed to register a particular type
struct EncodeMapperClassIORegistration {
  using Reader = std::unique_ptr<EncodeMapperClass> (*)(
      std::istream &stream, const std::string &source);

  using Creator = std::unique_ptr<EncodeMapperImplBase> (*)(uint8_t flags,
                                                            EncodeType type);

  using Entry = EncodeMapperClassRegEntry<Reader, Creator>;

  // EncodeMapper register.
  using Register = EncodeMapperClassIORegister<Reader, Creator>;

  // EncodeMapper register-er.
  using Registerer =
      GenericRegisterer<EncodeMapperClassIORegister<Reader, Creator>>;
};

#define REGISTER_ENCODEMAPPER_CLASS(Arc)             \
  static EncodeMapperClassIORegistration::Registerer \
      EncodeMapperClass_##Arc##_registerer(          \
          Arc::Type(),                               \
          EncodeMapperClassIORegistration::Entry(    \
              EncodeMapperClass::Read<Arc>, EncodeMapperClass::Create<Arc>));

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_ENCODEMAPPER_CLASS_H_
