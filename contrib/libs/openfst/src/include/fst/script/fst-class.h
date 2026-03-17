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

#ifndef FST_SCRIPT_FST_CLASS_H_
#define FST_SCRIPT_FST_CLASS_H_

#include <algorithm>
#include <cstdint>
#include <istream>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include <fst/expanded-fst.h>
#include <fst/fst.h>
#include <fst/generic-register.h>
#include <fst/mutable-fst.h>
#include <fst/vector-fst.h>
#include <fst/script/arc-class.h>
#include <fst/script/weight-class.h>
#include <string_view>

// Classes to support "boxing" all existing types of FST arcs in a single
// FstClass which hides the arc types. This allows clients to load
// and work with FSTs without knowing the arc type. These classes are only
// recommended for use in high-level scripting applications. Most users should
// use the lower-level templated versions corresponding to these classes.

namespace fst {
namespace script {

// Abstract base class defining the set of functionalities implemented in all
// impls and passed through by all bases. Below FstClassBase the class
// hierarchy bifurcates; FstClassImplBase serves as the base class for all
// implementations (of which FstClassImpl is currently the only one) and
// FstClass serves as the base class for all interfaces.

class FstClassBase {
 public:
  virtual const std::string &ArcType() const = 0;
  virtual WeightClass Final(int64_t) const = 0;
  virtual const std::string &FstType() const = 0;
  virtual const SymbolTable *InputSymbols() const = 0;
  virtual size_t NumArcs(int64_t) const = 0;
  virtual size_t NumInputEpsilons(int64_t) const = 0;
  virtual size_t NumOutputEpsilons(int64_t) const = 0;
  virtual const SymbolTable *OutputSymbols() const = 0;
  virtual uint64_t Properties(uint64_t, bool) const = 0;
  virtual int64_t Start() const = 0;
  virtual const std::string &WeightType() const = 0;
  virtual bool ValidStateId(int64_t) const = 0;
  virtual bool Write(const std::string &) const = 0;
  virtual bool Write(std::ostream &, const std::string &) const = 0;
  virtual ~FstClassBase() {}
};

// Adds all the MutableFst methods.
class FstClassImplBase : public FstClassBase {
 public:
  virtual bool AddArc(int64_t, const ArcClass &) = 0;
  virtual int64_t AddState() = 0;
  virtual void AddStates(size_t) = 0;
  virtual FstClassImplBase *Copy() = 0;
  virtual bool DeleteArcs(int64_t, size_t) = 0;
  virtual bool DeleteArcs(int64_t) = 0;
  virtual bool DeleteStates(const std::vector<int64_t> &) = 0;
  virtual void DeleteStates() = 0;
  virtual SymbolTable *MutableInputSymbols() = 0;
  virtual SymbolTable *MutableOutputSymbols() = 0;
  virtual int64_t NumStates() const = 0;
  virtual bool ReserveArcs(int64_t, size_t) = 0;
  virtual void ReserveStates(int64_t) = 0;
  virtual void SetInputSymbols(const SymbolTable *) = 0;
  virtual bool SetFinal(int64_t, const WeightClass &) = 0;
  virtual void SetOutputSymbols(const SymbolTable *) = 0;
  virtual void SetProperties(uint64_t, uint64_t) = 0;
  virtual bool SetStart(int64_t) = 0;
  ~FstClassImplBase() override {}
};

// Containiner class wrapping an Fst<Arc>, hiding its arc type. Whether this
// Fst<Arc> pointer refers to a special kind of FST (e.g. a MutableFst) is
// known by the type of interface class that owns the pointer to this
// container.

template <class Arc>
class FstClassImpl : public FstClassImplBase {
 public:
  explicit FstClassImpl(std::unique_ptr<Fst<Arc>> impl)
      : impl_(std::move(impl)) {}

  explicit FstClassImpl(const Fst<Arc> &impl) : impl_(impl.Copy()) {}

  // Warning: calling this method casts the FST to a mutable FST.
  bool AddArc(int64_t s, const ArcClass &ac) final {
    if (!ValidStateId(s)) return false;
    // Note that we do not check that the destination state is valid, so users
    // can add arcs before they add the corresponding states. Verify can be
    // used to determine whether any arc has a nonexisting destination.
    Arc arc(ac.ilabel, ac.olabel, *ac.weight.GetWeight<typename Arc::Weight>(),
            ac.nextstate);
    down_cast<MutableFst<Arc> *>(impl_.get())->AddArc(s, arc);
    return true;
  }

  // Warning: calling this method casts the FST to a mutable FST.
  int64_t AddState() final {
    return down_cast<MutableFst<Arc> *>(impl_.get())->AddState();
  }

  // Warning: calling this method casts the FST to a mutable FST.
  void AddStates(size_t n) final {
    return down_cast<MutableFst<Arc> *>(impl_.get())->AddStates(n);
  }

  const std::string &ArcType() const final { return Arc::Type(); }

  FstClassImpl *Copy() final { return new FstClassImpl<Arc>(*impl_); }

  // Warning: calling this method casts the FST to a mutable FST.
  bool DeleteArcs(int64_t s, size_t n) final {
    if (!ValidStateId(s)) return false;
    down_cast<MutableFst<Arc> *>(impl_.get())->DeleteArcs(s, n);
    return true;
  }

  // Warning: calling this method casts the FST to a mutable FST.
  bool DeleteArcs(int64_t s) final {
    if (!ValidStateId(s)) return false;
    down_cast<MutableFst<Arc> *>(impl_.get())->DeleteArcs(s);
    return true;
  }

  // Warning: calling this method casts the FST to a mutable FST.
  bool DeleteStates(const std::vector<int64_t> &dstates) final {
    for (const auto &state : dstates)
      if (!ValidStateId(state)) return false;
    // Warning: calling this method with any integers beyond the precision of
    // the underlying FST will result in truncation.
    std::vector<typename Arc::StateId> typed_dstates(dstates.size());
    std::copy(dstates.begin(), dstates.end(), typed_dstates.begin());
    down_cast<MutableFst<Arc> *>(impl_.get())->DeleteStates(typed_dstates);
    return true;
  }

  // Warning: calling this method casts the FST to a mutable FST.
  void DeleteStates() final {
    down_cast<MutableFst<Arc> *>(impl_.get())->DeleteStates();
  }

  WeightClass Final(int64_t s) const final {
    if (!ValidStateId(s)) return WeightClass::NoWeight(WeightType());
    WeightClass w(impl_->Final(s));
    return w;
  }

  const std::string &FstType() const final { return impl_->Type(); }

  const SymbolTable *InputSymbols() const final {
    return impl_->InputSymbols();
  }

  // Warning: calling this method casts the FST to a mutable FST.
  SymbolTable *MutableInputSymbols() final {
    return down_cast<MutableFst<Arc> *>(impl_.get())->MutableInputSymbols();
  }

  // Warning: calling this method casts the FST to a mutable FST.
  SymbolTable *MutableOutputSymbols() final {
    return down_cast<MutableFst<Arc> *>(impl_.get())->MutableOutputSymbols();
  }

  // Signals failure by returning size_t max.
  size_t NumArcs(int64_t s) const final {
    return ValidStateId(s) ? impl_->NumArcs(s)
                           : std::numeric_limits<size_t>::max();
  }

  // Signals failure by returning size_t max.
  size_t NumInputEpsilons(int64_t s) const final {
    return ValidStateId(s) ? impl_->NumInputEpsilons(s)
                           : std::numeric_limits<size_t>::max();
  }

  // Signals failure by returning size_t max.
  size_t NumOutputEpsilons(int64_t s) const final {
    return ValidStateId(s) ? impl_->NumOutputEpsilons(s)
                           : std::numeric_limits<size_t>::max();
  }

  // Warning: calling this method casts the FST to a mutable FST.
  int64_t NumStates() const final {
    return down_cast<MutableFst<Arc> *>(impl_.get())->NumStates();
  }

  uint64_t Properties(uint64_t mask, bool test) const final {
    return impl_->Properties(mask, test);
  }

  // Warning: calling this method casts the FST to a mutable FST.
  bool ReserveArcs(int64_t s, size_t n) final {
    if (!ValidStateId(s)) return false;
    down_cast<MutableFst<Arc> *>(impl_.get())->ReserveArcs(s, n);
    return true;
  }

  // Warning: calling this method casts the FST to a mutable FST.
  void ReserveStates(int64_t n) final {
    down_cast<MutableFst<Arc> *>(impl_.get())->ReserveStates(n);
  }

  const SymbolTable *OutputSymbols() const final {
    return impl_->OutputSymbols();
  }

  // Warning: calling this method casts the FST to a mutable FST.
  void SetInputSymbols(const SymbolTable *isyms) final {
    down_cast<MutableFst<Arc> *>(impl_.get())->SetInputSymbols(isyms);
  }

  // Warning: calling this method casts the FST to a mutable FST.
  bool SetFinal(int64_t s, const WeightClass &weight) final {
    if (!ValidStateId(s)) return false;
    down_cast<MutableFst<Arc> *>(impl_.get())
        ->SetFinal(s, *weight.GetWeight<typename Arc::Weight>());
    return true;
  }

  // Warning: calling this method casts the FST to a mutable FST.
  void SetOutputSymbols(const SymbolTable *osyms) final {
    down_cast<MutableFst<Arc> *>(impl_.get())->SetOutputSymbols(osyms);
  }

  // Warning: calling this method casts the FST to a mutable FST.
  void SetProperties(uint64_t props, uint64_t mask) final {
    down_cast<MutableFst<Arc> *>(impl_.get())->SetProperties(props, mask);
  }

  // Warning: calling this method casts the FST to a mutable FST.
  bool SetStart(int64_t s) final {
    if (!ValidStateId(s)) return false;
    down_cast<MutableFst<Arc> *>(impl_.get())->SetStart(s);
    return true;
  }

  int64_t Start() const final { return impl_->Start(); }

  bool ValidStateId(int64_t s) const final {
    // This cowardly refuses to count states if the FST is not yet expanded.
    if (!Properties(kExpanded, true)) {
      FSTERROR() << "Cannot get number of states for unexpanded FST";
      return false;
    }
    // If the FST is already expanded, CountStates calls NumStates.
    if (s < 0 || s >= CountStates(*impl_)) {
      FSTERROR() << "State ID " << s << " not valid";
      return false;
    }
    return true;
  }

  const std::string &WeightType() const final { return Arc::Weight::Type(); }

  bool Write(const std::string &source) const final {
    return impl_->Write(source);
  }

  bool Write(std::ostream &ostr, const std::string &source) const final {
    const FstWriteOptions opts(source);
    return impl_->Write(ostr, opts);
  }

  ~FstClassImpl() override {}

  Fst<Arc> *GetImpl() const { return impl_.get(); }

 private:
  std::unique_ptr<Fst<Arc>> impl_;
};

// BASE CLASS DEFINITIONS

class MutableFstClass;

class FstClass : public FstClassBase {
 public:
  FstClass() : impl_(nullptr) {}

  template <class Arc>
  explicit FstClass(std::unique_ptr<Fst<Arc>> fst)
      : impl_(std::make_unique<FstClassImpl<Arc>>(std::move(fst))) {}

  template <class Arc>
  explicit FstClass(const Fst<Arc> &fst)
      : impl_(std::make_unique<FstClassImpl<Arc>>(fst)) {}

  FstClass(const FstClass &other)
      : impl_(other.impl_ == nullptr ? nullptr : other.impl_->Copy()) {}

  FstClass &operator=(const FstClass &other) {
    impl_.reset(other.impl_ == nullptr ? nullptr : other.impl_->Copy());
    return *this;
  }

  WeightClass Final(int64_t s) const final { return impl_->Final(s); }

  const std::string &ArcType() const final { return impl_->ArcType(); }

  const std::string &FstType() const final { return impl_->FstType(); }

  const SymbolTable *InputSymbols() const final {
    return impl_->InputSymbols();
  }

  size_t NumArcs(int64_t s) const final { return impl_->NumArcs(s); }

  size_t NumInputEpsilons(int64_t s) const final {
    return impl_->NumInputEpsilons(s);
  }

  size_t NumOutputEpsilons(int64_t s) const final {
    return impl_->NumOutputEpsilons(s);
  }

  const SymbolTable *OutputSymbols() const final {
    return impl_->OutputSymbols();
  }

  uint64_t Properties(uint64_t mask, bool test) const final {
    // Special handling for FSTs with a null impl.
    if (!impl_) return kError & mask;
    return impl_->Properties(mask, test);
  }

  static std::unique_ptr<FstClass> Read(const std::string &source);

  static std::unique_ptr<FstClass> Read(std::istream &istrm,
                                        const std::string &source);

  int64_t Start() const final { return impl_->Start(); }

  bool ValidStateId(int64_t s) const final { return impl_->ValidStateId(s); }

  const std::string &WeightType() const final { return impl_->WeightType(); }

  // Helper that logs an ERROR if the weight type of an FST and a WeightClass
  // don't match.

  bool WeightTypesMatch(const WeightClass &weight,
                        std::string_view op_name) const;

  bool Write(const std::string &source) const final {
    return impl_->Write(source);
  }

  bool Write(std::ostream &ostr, const std::string &source) const final {
    return impl_->Write(ostr, source);
  }

  ~FstClass() override {}

  // These methods are required by IO registration.

  template <class Arc>
  static std::unique_ptr<FstClassImplBase> Convert(const FstClass &other) {
    FSTERROR() << "Doesn't make sense to convert any class to type FstClass";
    return nullptr;
  }

  template <class Arc>
  static std::unique_ptr<FstClassImplBase> Create() {
    FSTERROR() << "Doesn't make sense to create an FstClass with a "
               << "particular arc type";
    return nullptr;
  }

  template <class Arc>
  const Fst<Arc> *GetFst() const {
    if (Arc::Type() != ArcType()) {
      return nullptr;
    } else {
      FstClassImpl<Arc> *typed_impl =
          down_cast<FstClassImpl<Arc> *>(impl_.get());
      return typed_impl->GetImpl();
    }
  }

  template <class Arc>
  static std::unique_ptr<FstClass> Read(std::istream &stream,
                                        const FstReadOptions &opts) {
    if (!opts.header) {
      LOG(ERROR) << "FstClass::Read: Options header not specified";
      return nullptr;
    }
    const FstHeader &hdr = *opts.header;
    if (hdr.Properties() & kMutable) {
      return ReadTypedFst<MutableFstClass, MutableFst<Arc>>(stream, opts);
    } else {
      return ReadTypedFst<FstClass, Fst<Arc>>(stream, opts);
    }
  }

 protected:
  explicit FstClass(std::unique_ptr<FstClassImplBase> impl)
      : impl_(std::move(impl)) {}

  const FstClassImplBase *GetImpl() const { return impl_.get(); }

  FstClassImplBase *GetImpl() { return impl_.get(); }

  // Generic template method for reading an arc-templated FST of type
  // UnderlyingT, and returning it wrapped as FstClassT, with appropriate
  // error checking. Called from arc-templated Read() static methods.
  template <class FstClassT, class UnderlyingT>
  static std::unique_ptr<FstClassT> ReadTypedFst(std::istream &stream,
                                                 const FstReadOptions &opts) {
    std::unique_ptr<UnderlyingT> u(UnderlyingT::Read(stream, opts));
    return u ? std::make_unique<FstClassT>(std::move(u)) : nullptr;
  }

 private:
  std::unique_ptr<FstClassImplBase> impl_;
};

// Specific types of FstClass with special properties

class MutableFstClass : public FstClass {
 public:
  bool AddArc(int64_t s, const ArcClass &ac) {
    if (!WeightTypesMatch(ac.weight, "AddArc")) return false;
    return GetImpl()->AddArc(s, ac);
  }

  int64_t AddState() { return GetImpl()->AddState(); }

  void AddStates(size_t n) { return GetImpl()->AddStates(n); }

  bool DeleteArcs(int64_t s, size_t n) { return GetImpl()->DeleteArcs(s, n); }

  bool DeleteArcs(int64_t s) { return GetImpl()->DeleteArcs(s); }

  bool DeleteStates(const std::vector<int64_t> &dstates) {
    return GetImpl()->DeleteStates(dstates);
  }

  void DeleteStates() { GetImpl()->DeleteStates(); }

  SymbolTable *MutableInputSymbols() {
    return GetImpl()->MutableInputSymbols();
  }

  SymbolTable *MutableOutputSymbols() {
    return GetImpl()->MutableOutputSymbols();
  }

  int64_t NumStates() const { return GetImpl()->NumStates(); }

  bool ReserveArcs(int64_t s, size_t n) { return GetImpl()->ReserveArcs(s, n); }

  void ReserveStates(int64_t n) { GetImpl()->ReserveStates(n); }

  static std::unique_ptr<MutableFstClass> Read(const std::string &source,
                                               bool convert = false);

  void SetInputSymbols(const SymbolTable *isyms) {
    GetImpl()->SetInputSymbols(isyms);
  }

  bool SetFinal(int64_t s, const WeightClass &weight) {
    if (!WeightTypesMatch(weight, "SetFinal")) return false;
    return GetImpl()->SetFinal(s, weight);
  }

  void SetOutputSymbols(const SymbolTable *osyms) {
    GetImpl()->SetOutputSymbols(osyms);
  }

  void SetProperties(uint64_t props, uint64_t mask) {
    GetImpl()->SetProperties(props, mask);
  }

  bool SetStart(int64_t s) { return GetImpl()->SetStart(s); }

  template <class Arc>
  explicit MutableFstClass(std::unique_ptr<MutableFst<Arc>> fst)
      // NB: The natural cast-less way to do this doesn't compile for some
      // arcane reason.
      : FstClass(
            fst::implicit_cast<std::unique_ptr<Fst<Arc>>>(std::move(fst))) {}

  template <class Arc>
  explicit MutableFstClass(const MutableFst<Arc> &fst) : FstClass(fst) {}

  // These methods are required by IO registration.

  template <class Arc>
  static std::unique_ptr<FstClassImplBase> Convert(const FstClass &other) {
    FSTERROR() << "Doesn't make sense to convert any class to type "
               << "MutableFstClass";
    return nullptr;
  }

  template <class Arc>
  static std::unique_ptr<FstClassImplBase> Create() {
    FSTERROR() << "Doesn't make sense to create a MutableFstClass with a "
               << "particular arc type";
    return nullptr;
  }

  template <class Arc>
  MutableFst<Arc> *GetMutableFst() {
    Fst<Arc> *fst = const_cast<Fst<Arc> *>(this->GetFst<Arc>());
    MutableFst<Arc> *mfst = down_cast<MutableFst<Arc> *>(fst);
    return mfst;
  }

  template <class Arc>
  static std::unique_ptr<MutableFstClass> Read(std::istream &stream,
                                               const FstReadOptions &opts) {
    std::unique_ptr<MutableFst<Arc>> mfst(MutableFst<Arc>::Read(stream, opts));
    return mfst ? std::make_unique<MutableFstClass>(std::move(mfst)) : nullptr;
  }

 protected:
  explicit MutableFstClass(std::unique_ptr<FstClassImplBase> impl)
      : FstClass(std::move(impl)) {}
};

class VectorFstClass : public MutableFstClass {
 public:
  explicit VectorFstClass(std::unique_ptr<FstClassImplBase> impl)
      : MutableFstClass(std::move(impl)) {}

  explicit VectorFstClass(const FstClass &other);

  explicit VectorFstClass(std::string_view arc_type);

  static std::unique_ptr<VectorFstClass> Read(const std::string &source);

  template <class Arc>
  static std::unique_ptr<VectorFstClass> Read(std::istream &stream,
                                              const FstReadOptions &opts) {
    std::unique_ptr<VectorFst<Arc>> vfst(VectorFst<Arc>::Read(stream, opts));
    return vfst ? std::make_unique<VectorFstClass>(std::move(vfst)) : nullptr;
  }

  template <class Arc>
  explicit VectorFstClass(std::unique_ptr<VectorFst<Arc>> fst)
      // NB: The natural cast-less way to do this doesn't compile for some
      // arcane reason.
      : MutableFstClass(fst::implicit_cast<std::unique_ptr<MutableFst<Arc>>>(
            std::move(fst))) {}

  template <class Arc>
  explicit VectorFstClass(const VectorFst<Arc> &fst) : MutableFstClass(fst) {}

  template <class Arc>
  static std::unique_ptr<FstClassImplBase> Convert(const FstClass &other) {
    return std::make_unique<FstClassImpl<Arc>>(
        std::make_unique<VectorFst<Arc>>(*other.GetFst<Arc>()));
  }

  template <class Arc>
  static std::unique_ptr<FstClassImplBase> Create() {
    return std::make_unique<FstClassImpl<Arc>>(
        std::make_unique<VectorFst<Arc>>());
  }
};

// Registration stuff.

// This class definition is to avoid a nested class definition inside the
// FstClassIORegistration struct.
template <class Reader, class Creator, class Converter>
struct FstClassRegEntry {
  Reader reader;
  Creator creator;
  Converter converter;

  FstClassRegEntry(Reader r, Creator cr, Converter co)
      : reader(r), creator(cr), converter(co) {}

  FstClassRegEntry() : reader(nullptr), creator(nullptr), converter(nullptr) {}
};

// Actual FST IO method register.
template <class Reader, class Creator, class Converter>
class FstClassIORegister
    : public GenericRegister<std::string,
                             FstClassRegEntry<Reader, Creator, Converter>,
                             FstClassIORegister<Reader, Creator, Converter>> {
 public:
  Reader GetReader(std::string_view arc_type) const {
    return this->GetEntry(arc_type).reader;
  }

  Creator GetCreator(std::string_view arc_type) const {
    return this->GetEntry(arc_type).creator;
  }

  Converter GetConverter(std::string_view arc_type) const {
    return this->GetEntry(arc_type).converter;
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
// of FST class (e.g., a plain FstClass, or a MutableFstClass, etc.).
template <class FstClassType>
struct FstClassIORegistration {
  using Reader = std::unique_ptr<FstClassType> (*)(std::istream &stream,
                                                   const FstReadOptions &opts);

  using Creator = std::unique_ptr<FstClassImplBase> (*)();

  using Converter =
      std::unique_ptr<FstClassImplBase> (*)(const FstClass &other);

  using Entry = FstClassRegEntry<Reader, Creator, Converter>;

  // FST class Register.
  using Register = FstClassIORegister<Reader, Creator, Converter>;

  // FST class Register-er.
  using Registerer =
      GenericRegisterer<FstClassIORegister<Reader, Creator, Converter>>;
};

// Macros for registering other arc types.

#define REGISTER_FST_CLASS(Class, Arc)                                         \
  static FstClassIORegistration<Class>::Registerer Class##_##Arc##_registerer( \
      Arc::Type(),                                                             \
      FstClassIORegistration<Class>::Entry(                                    \
          Class::Read<Arc>, Class::Create<Arc>, Class::Convert<Arc>))

#define REGISTER_FST_CLASSES(Arc)           \
  REGISTER_FST_CLASS(FstClass, Arc);        \
  REGISTER_FST_CLASS(MutableFstClass, Arc); \
  REGISTER_FST_CLASS(VectorFstClass, Arc);

}  // namespace script
}  // namespace fst

#endif  // FST_SCRIPT_FST_CLASS_H_
