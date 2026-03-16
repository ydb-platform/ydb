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


#ifndef PYNINI_PATHSSCRIPT_H_
#define PYNINI_PATHSSCRIPT_H_

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include <fst/script/arg-packs.h>
#include <fst/script/fstscript.h>
#include "paths.h"

namespace fst {
namespace script {

// Virtual interface implemented by each concrete StatesImpl<F>.
class StringPathIteratorImplBase {
 public:
  virtual bool Done() const = 0;
  virtual bool Error() const = 0;
  virtual void ILabels(std::vector<int64_t> *labels) const = 0;
  virtual std::vector<int64_t> ILabels() const = 0;
  virtual void IString(std::string *result) const = 0;
  virtual std::string IString() const = 0;
  virtual void Next() = 0;
  virtual void OLabels(std::vector<int64_t> *labels) const = 0;
  virtual std::vector<int64_t> OLabels() const = 0;
  virtual void OString(std::string *result) const = 0;
  virtual std::string OString() const = 0;
  virtual void Reset() = 0;
  virtual WeightClass Weight() const = 0;
  virtual ~StringPathIteratorImplBase() {}
};

// Templated implementation.
template <class Arc>
class StringPathIteratorImpl : public StringPathIteratorImplBase {
 public:
  using Label = typename Arc::Label;

  explicit StringPathIteratorImpl(const Fst<Arc> &fst,
                                  TokenType input_token_type = TokenType::BYTE,
                                  TokenType output_token_type = TokenType::BYTE,
                                  const SymbolTable *input_symbols = nullptr,
                                  const SymbolTable *output_symbols = nullptr)
      : impl_(new StringPathIterator<Arc>(fst, input_token_type,
                                          output_token_type, input_symbols,
                                          output_symbols)) {}

  bool Done() const override { return impl_->Done(); }

  bool Error() const override { return impl_->Error(); }

  void ILabels(std::vector<int64_t> *labels) const override {
    const auto &typed_labels = impl_->ILabels();
    labels->clear();
    labels->resize(typed_labels.size());
    std::copy(typed_labels.begin(), typed_labels.end(), labels->begin());
  }

  std::vector<int64_t> ILabels() const override {
    std::vector<int64_t> labels;
    ILabels(&labels);
    return labels;
  }

  void IString(std::string *result) const override { impl_->IString(result); }

  std::string IString() const override { return impl_->IString(); }

  void Next() override { impl_->Next(); }

  void Reset() override { impl_->Reset(); }

  void OLabels(std::vector<int64_t> *labels) const override {
    const auto &typed_labels = impl_->OLabels();
    labels->clear();
    labels->resize(typed_labels.size());
    std::copy(typed_labels.begin(), typed_labels.end(), labels->begin());
  }

  std::vector<int64_t> OLabels() const override {
    std::vector<int64_t> labels;
    OLabels(&labels);
    return labels;
  }

  void OString(std::string *result) const override { impl_->OString(result); }

  std::string OString() const override { return impl_->OString(); }

  WeightClass Weight() const override { return WeightClass(impl_->Weight()); }

 private:
  std::unique_ptr<StringPathIterator<Arc>> impl_;
};

class StringPathIteratorClass;

using InitStringPathIteratorClassArgs =
    std::tuple<const FstClass &, TokenType, TokenType, const SymbolTable *,
               const SymbolTable *, StringPathIteratorClass *>;

// Untemplated user-facing class holding templated pimpl.
class StringPathIteratorClass {
 public:
  explicit StringPathIteratorClass(
      const FstClass &fst, TokenType input_token_type = TokenType::BYTE,
      TokenType output_token_type = TokenType::BYTE,
      const SymbolTable *input_symbols = nullptr,
      const SymbolTable *output_symbols = nullptr);

  // Same as above, but applies the same string token type and symbol table
  // to both tapes.
  StringPathIteratorClass(const FstClass &fst, TokenType type,
                          const SymbolTable *symbols = nullptr)
      : StringPathIteratorClass(fst, type, type, symbols, symbols) {}

  bool Done() const { return impl_->Done(); }

  bool Error() const { return impl_->Error(); }

  void ILabels(std::vector<int64_t> *labels) const { impl_->ILabels(labels); }

  std::vector<int64_t> ILabels() const { return impl_->ILabels(); }

  template <class Arc>
  friend void InitStringPathIteratorClass(
      InitStringPathIteratorClassArgs *args);

  void IString(std::string *result) const { impl_->IString(result); }

  std::string IString() const { return impl_->IString(); }

  void Next() { impl_->Next(); }

  void Reset() { impl_->Reset(); }

  void OLabels(std::vector<int64_t> *labels) const { impl_->OLabels(labels); }

  std::vector<int64_t> OLabels() const { return impl_->OLabels(); }

  std::string OString() const { return impl_->OString(); }

  WeightClass Weight() const { return (impl_->Weight()); }

 private:
  std::unique_ptr<StringPathIteratorImplBase> impl_;
};

template <class Arc>
void InitStringPathIteratorClass(InitStringPathIteratorClassArgs *args) {
  const Fst<Arc> &fst = *(std::get<0>(*args).GetFst<Arc>());
  std::get<5>(*args)->impl_.reset(new StringPathIteratorImpl<Arc>(
      fst, std::get<1>(*args), std::get<2>(*args), std::get<3>(*args),
      std::get<4>(*args)));
}

}  // namespace script
}  // namespace fst

#endif  // PYNINI_PATHSSCRIPT_H_

