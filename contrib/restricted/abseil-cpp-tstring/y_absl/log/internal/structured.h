// Copyright 2022 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// File: log/internal/structured.h
// -----------------------------------------------------------------------------

#ifndef Y_ABSL_LOG_INTERNAL_STRUCTURED_H_
#define Y_ABSL_LOG_INTERNAL_STRUCTURED_H_

#include <ostream>
#include <util/generic/string.h>

#include "y_absl/base/attributes.h"
#include "y_absl/base/config.h"
#include "y_absl/functional/any_invocable.h"
#include "y_absl/log/internal/log_message.h"
#include "y_absl/log/internal/structured_proto.h"
#include "y_absl/strings/str_cat.h"
#include "y_absl/strings/string_view.h"

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN
namespace log_internal {

class Y_ABSL_MUST_USE_RESULT AsLiteralImpl final {
 public:
  explicit AsLiteralImpl(y_absl::string_view str Y_ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : str_(str) {}
  AsLiteralImpl(const AsLiteralImpl&) = default;
  AsLiteralImpl& operator=(const AsLiteralImpl&) = default;

 private:
  y_absl::string_view str_;

  friend std::ostream& operator<<(std::ostream& os,
                                  AsLiteralImpl&& as_literal) {
    return os << as_literal.str_;
  }
  void AddToMessage(log_internal::LogMessage& m) {
    m.CopyToEncodedBuffer<log_internal::LogMessage::StringType::kLiteral>(str_);
  }
  friend log_internal::LogMessage& operator<<(log_internal::LogMessage& m,
                                              AsLiteralImpl as_literal) {
    as_literal.AddToMessage(m);
    return m;
  }
};

enum class StructuredStringType {
  kLiteral,
  kNotLiteral,
};

// Structured log data for a string and associated structured proto field,
// both of which must outlive this object.
template <StructuredStringType str_type>
class Y_ABSL_MUST_USE_RESULT AsStructuredStringTypeImpl final {
 public:
  constexpr AsStructuredStringTypeImpl(
      y_absl::string_view str Y_ABSL_ATTRIBUTE_LIFETIME_BOUND,
      StructuredProtoField field Y_ABSL_ATTRIBUTE_LIFETIME_BOUND)
      : str_(str), field_(field) {}

 private:
  y_absl::string_view str_;
  StructuredProtoField field_;

  friend std::ostream& operator<<(std::ostream& os,
                                  const AsStructuredStringTypeImpl& impl) {
    return os << impl.str_;
  }
  void AddToMessage(LogMessage& m) const {
    if (str_type == StructuredStringType::kLiteral) {
      return m.CopyToEncodedBufferWithStructuredProtoField<
          log_internal::LogMessage::StringType::kLiteral>(field_, str_);
    } else {
      return m.CopyToEncodedBufferWithStructuredProtoField<
          log_internal::LogMessage::StringType::kNotLiteral>(field_, str_);
    }
  }
  friend LogMessage& operator<<(LogMessage& m,
                                const AsStructuredStringTypeImpl& impl) {
    impl.AddToMessage(m);
    return m;
  }
};

using AsStructuredLiteralImpl =
    AsStructuredStringTypeImpl<StructuredStringType::kLiteral>;
using AsStructuredNotLiteralImpl =
    AsStructuredStringTypeImpl<StructuredStringType::kNotLiteral>;

// Structured log data for a stringifyable type T and associated structured
// proto field, both of which must outlive this object.
template <typename T>
class Y_ABSL_MUST_USE_RESULT AsStructuredValueImpl final {
 public:
  using ValueFormatter = y_absl::AnyInvocable<TString(T) const>;

  constexpr AsStructuredValueImpl(
      T value Y_ABSL_ATTRIBUTE_LIFETIME_BOUND,
      StructuredProtoField field Y_ABSL_ATTRIBUTE_LIFETIME_BOUND,
      ValueFormatter value_formatter =
          [](T value) { return y_absl::StrCat(value); })
      : value_(value),
        field_(field),
        value_formatter_(std::move(value_formatter)) {}

 private:
  T value_;
  StructuredProtoField field_;
  ValueFormatter value_formatter_;

  friend std::ostream& operator<<(std::ostream& os,
                                  const AsStructuredValueImpl& impl) {
    return os << impl.value_formatter_(impl.value_);
  }
  void AddToMessage(LogMessage& m) const {
    m.CopyToEncodedBufferWithStructuredProtoField<
        log_internal::LogMessage::StringType::kNotLiteral>(
        field_, value_formatter_(value_));
  }
  friend LogMessage& operator<<(LogMessage& m,
                                const AsStructuredValueImpl& impl) {
    impl.AddToMessage(m);
    return m;
  }
};

#ifdef Y_ABSL_HAVE_CLASS_TEMPLATE_ARGUMENT_DEDUCTION

// Template deduction guide so `AsStructuredValueImpl(42, data)` works
// without specifying the template type.
template <typename T>
AsStructuredValueImpl(T value, StructuredProtoField field)
    -> AsStructuredValueImpl<T>;

// Template deduction guide so `AsStructuredValueImpl(42, data, formatter)`
// works without specifying the template type.
template <typename T>
AsStructuredValueImpl(
    T value, StructuredProtoField field,
    typename AsStructuredValueImpl<T>::ValueFormatter value_formatter)
    -> AsStructuredValueImpl<T>;

#endif  // Y_ABSL_HAVE_CLASS_TEMPLATE_ARGUMENT_DEDUCTION

}  // namespace log_internal
Y_ABSL_NAMESPACE_END
}  // namespace y_absl

#endif  // Y_ABSL_LOG_INTERNAL_STRUCTURED_H_
