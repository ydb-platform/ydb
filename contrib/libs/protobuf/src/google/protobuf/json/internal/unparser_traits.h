// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef GOOGLE_PROTOBUF_JSON_INTERNAL_UNPARSER_TRAITS_H__
#define GOOGLE_PROTOBUF_JSON_INTERNAL_UNPARSER_TRAITS_H__

#include <algorithm>
#include <cfloat>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "google/protobuf/type.pb.h"
#include "y_absl/container/flat_hash_map.h"
#include "y_absl/status/status.h"
#include "y_absl/strings/escaping.h"
#include "y_absl/strings/numbers.h"
#include "y_absl/strings/str_format.h"
#include "y_absl/strings/string_view.h"
#include "y_absl/types/optional.h"
#include "y_absl/types/variant.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/json/internal/descriptor_traits.h"
#include "google/protobuf/stubs/status_macros.h"

// Must be included last.
#include "google/protobuf/port_def.inc"

namespace google {
namespace protobuf {
namespace json_internal {
// The type traits in this file provide describe how to read from protobuf
// representation used by the JSON API, either via proto reflection or via
// something ad-hoc for type.proto.

// Helper alias templates to avoid needing to write `typename` in function
// signatures.
template <typename Traits>
using Msg = typename Traits::Msg;

// Traits for proto2-ish deserialization.
struct UnparseProto2Descriptor : Proto2Descriptor {
  // A message value that fields can be read from.
  using Msg = Message;

  static const Desc& GetDesc(const Msg& msg) { return *msg.GetDescriptor(); }

  // Appends extension fields to `fields`.
  static void FindAndAppendExtensions(const Msg& msg,
                                      std::vector<Field>& fields) {
    // Note that it is *not* correct to use ListFields for getting a list of
    // fields to write, because the way that JSON decides to print non-extension
    // fields is slightly subtle. That logic is handled elsewhere; we're only
    // here to get extensions.
    std::vector<Field> all_fields;
    msg.GetReflection()->ListFields(msg, &all_fields);

    for (Field field : all_fields) {
      if (field->is_extension()) {
        fields.push_back(field);
      }
    }
  }

  static size_t GetSize(Field f, const Msg& msg) {
    if (f->is_repeated()) {
      return msg.GetReflection()->FieldSize(msg, f);
    } else {
      return msg.GetReflection()->HasField(msg, f) ? 1 : 0;
    }
  }

  static y_absl::StatusOr<float> GetFloat(Field f) {
    return f->default_value_float();
  }

  static y_absl::StatusOr<double> GetDouble(Field f) {
    return f->default_value_double();
  }

  static y_absl::StatusOr<arc_i32> GetInt32(Field f) {
    return f->default_value_int32();
  }

  static y_absl::StatusOr<arc_ui32> GetUInt32(Field f) {
    return f->default_value_uint32();
  }

  static y_absl::StatusOr<arc_i64> GetInt64(Field f) {
    return f->default_value_int64();
  }

  static y_absl::StatusOr<arc_ui64> GetUInt64(Field f) {
    return f->default_value_uint64();
  }

  static y_absl::StatusOr<bool> GetBool(Field f) {
    return f->default_value_bool();
  }

  static y_absl::StatusOr<arc_i32> GetEnumValue(Field f) {
    return f->default_value_enum()->number();
  }

  static y_absl::StatusOr<y_absl::string_view> GetString(Field f,
                                                     TProtoStringType& scratch) {
    return f->default_value_string();
  }

  static y_absl::StatusOr<const Msg*> GetMessage(Field f) {
    return y_absl::InternalError("message fields cannot have defaults");
  }

  static y_absl::StatusOr<float> GetFloat(Field f, const Msg& msg) {
    return msg.GetReflection()->GetFloat(msg, f);
  }

  static y_absl::StatusOr<double> GetDouble(Field f, const Msg& msg) {
    return msg.GetReflection()->GetDouble(msg, f);
  }

  static y_absl::StatusOr<arc_i32> GetInt32(Field f, const Msg& msg) {
    return msg.GetReflection()->GetInt32(msg, f);
  }

  static y_absl::StatusOr<arc_ui32> GetUInt32(Field f, const Msg& msg) {
    return msg.GetReflection()->GetUInt32(msg, f);
  }

  static y_absl::StatusOr<arc_i64> GetInt64(Field f, const Msg& msg) {
    return msg.GetReflection()->GetInt64(msg, f);
  }

  static y_absl::StatusOr<arc_ui64> GetUInt64(Field f, const Msg& msg) {
    return msg.GetReflection()->GetUInt64(msg, f);
  }

  static y_absl::StatusOr<bool> GetBool(Field f, const Msg& msg) {
    return msg.GetReflection()->GetBool(msg, f);
  }

  static y_absl::StatusOr<arc_i32> GetEnumValue(Field f, const Msg& msg) {
    return msg.GetReflection()->GetEnumValue(msg, f);
  }

  static y_absl::StatusOr<y_absl::string_view> GetString(Field f,
                                                     TProtoStringType& scratch,
                                                     const Msg& msg) {
    return msg.GetReflection()->GetStringReference(msg, f, &scratch);
  }

  static y_absl::StatusOr<const Msg*> GetMessage(Field f, const Msg& msg) {
    return &msg.GetReflection()->GetMessage(msg, f);
  }

  static y_absl::StatusOr<float> GetFloat(Field f, const Msg& msg, size_t idx) {
    return msg.GetReflection()->GetRepeatedFloat(msg, f, idx);
  }

  static y_absl::StatusOr<double> GetDouble(Field f, const Msg& msg, size_t idx) {
    return msg.GetReflection()->GetRepeatedDouble(msg, f, idx);
  }

  static y_absl::StatusOr<arc_i32> GetInt32(Field f, const Msg& msg, size_t idx) {
    return msg.GetReflection()->GetRepeatedInt32(msg, f, idx);
  }

  static y_absl::StatusOr<arc_ui32> GetUInt32(Field f, const Msg& msg,
                                            size_t idx) {
    return msg.GetReflection()->GetRepeatedUInt32(msg, f, idx);
  }

  static y_absl::StatusOr<arc_i64> GetInt64(Field f, const Msg& msg, size_t idx) {
    return msg.GetReflection()->GetRepeatedInt64(msg, f, idx);
  }

  static y_absl::StatusOr<arc_ui64> GetUInt64(Field f, const Msg& msg,
                                            size_t idx) {
    return msg.GetReflection()->GetRepeatedUInt64(msg, f, idx);
  }

  static y_absl::StatusOr<bool> GetBool(Field f, const Msg& msg, size_t idx) {
    return msg.GetReflection()->GetRepeatedBool(msg, f, idx);
  }

  static y_absl::StatusOr<arc_i32> GetEnumValue(Field f, const Msg& msg,
                                              size_t idx) {
    return msg.GetReflection()->GetRepeatedEnumValue(msg, f, idx);
  }

  static y_absl::StatusOr<y_absl::string_view> GetString(Field f,
                                                     TProtoStringType& scratch,
                                                     const Msg& msg,
                                                     size_t idx) {
    return msg.GetReflection()->GetRepeatedStringReference(msg, f, idx,
                                                           &scratch);
  }

  static y_absl::StatusOr<const Msg*> GetMessage(Field f, const Msg& msg,
                                               size_t idx) {
    return &msg.GetReflection()->GetRepeatedMessage(msg, f, idx);
  }

  template <typename F>
  static y_absl::Status WithDecodedMessage(const Desc& desc,
                                         y_absl::string_view data, F body) {
    DynamicMessageFactory factory;
    std::unique_ptr<Message> unerased(factory.GetPrototype(&desc)->New());
    unerased->ParsePartialFromString(data);

    // Explicitly create a const reference, so that we do not accidentally pass
    // a mutable reference to `body`.
    const Msg& ref = *unerased;
    return body(ref);
  }
};

struct UnparseProto3Type : Proto3Type {
  using Msg = UntypedMessage;

  static const Desc& GetDesc(const Msg& msg) { return msg.desc(); }

  static void FindAndAppendExtensions(const Msg&, std::vector<Field>&) {
    // type.proto does not support extensions.
  }

  static size_t GetSize(Field f, const Msg& msg) {
    return msg.Count(f->proto().number());
  }

  static y_absl::StatusOr<float> GetFloat(Field f) {
    if (f->proto().default_value().empty()) {
      return 0.0;
    }
    float x;
    if (!y_absl::SimpleAtof(f->proto().default_value(), &x)) {
      return y_absl::InternalError(y_absl::StrCat(
          "bad default value in type.proto: ", f->parent().proto().name()));
    }
    return x;
  }

  static y_absl::StatusOr<double> GetDouble(Field f) {
    if (f->proto().default_value().empty()) {
      return 0.0;
    }
    double x;
    if (!y_absl::SimpleAtod(f->proto().default_value(), &x)) {
      return y_absl::InternalError(y_absl::StrCat(
          "bad default value in type.proto: ", f->parent().proto().name()));
    }
    return x;
  }

  static y_absl::StatusOr<arc_i32> GetInt32(Field f) {
    if (f->proto().default_value().empty()) {
      return 0;
    }
    arc_i32 x;
    if (!y_absl::SimpleAtoi(f->proto().default_value(), &x)) {
      return y_absl::InternalError(y_absl::StrCat(
          "bad default value in type.proto: ", f->parent().proto().name()));
    }
    return x;
  }

  static y_absl::StatusOr<arc_ui32> GetUInt32(Field f) {
    if (f->proto().default_value().empty()) {
      return 0;
    }
    arc_ui32 x;
    if (!y_absl::SimpleAtoi(f->proto().default_value(), &x)) {
      return y_absl::InternalError(y_absl::StrCat(
          "bad default value in type.proto: ", f->parent().proto().name()));
    }
    return x;
  }

  static y_absl::StatusOr<arc_i64> GetInt64(Field f) {
    if (f->proto().default_value().empty()) {
      return 0;
    }
    arc_i64 x;
    if (!y_absl::SimpleAtoi(f->proto().default_value(), &x)) {
      return y_absl::InternalError(y_absl::StrCat(
          "bad default value in type.proto: ", f->parent().proto().name()));
    }
    return x;
  }

  static y_absl::StatusOr<arc_ui64> GetUInt64(Field f) {
    if (f->proto().default_value().empty()) {
      return 0;
    }
    arc_ui64 x;
    if (!y_absl::SimpleAtoi(f->proto().default_value(), &x)) {
      return y_absl::InternalError(y_absl::StrCat(
          "bad default value in type.proto: ", f->parent().proto().name()));
    }
    return x;
  }

  static y_absl::StatusOr<bool> GetBool(Field f) {
    if (f->proto().default_value().empty()) {
      return false;
    } else if (f->proto().default_value() == "false") {
      return false;
    } else if (f->proto().default_value() == "true") {
      return true;
    } else {
      return y_absl::InternalError(y_absl::StrCat(
          "bad default value in type.proto: ", f->parent().proto().name()));
    }
  }

  static y_absl::StatusOr<arc_i32> GetEnumValue(Field f) {
    if (f->proto().default_value().empty()) {
      auto e = f->EnumType();
      RETURN_IF_ERROR(e.status());

      return (**e).proto().enumvalue(0).number();
    }
    return EnumNumberByName(f, f->proto().default_value(),
                            /*case_insensitive=*/false);
  }

  static y_absl::StatusOr<y_absl::string_view> GetString(Field f,
                                                     TProtoStringType& scratch) {
    y_absl::CUnescape(f->proto().default_value(), &scratch);
    return scratch;
  }

  static y_absl::StatusOr<const Msg*> GetMessage(Field f) {
    return y_absl::InternalError("message fields cannot have defaults");
  }

  static y_absl::StatusOr<float> GetFloat(Field f, const Msg& msg,
                                        size_t idx = 0) {
    return msg.Get<float>(f->proto().number())[idx];
  }

  static y_absl::StatusOr<double> GetDouble(Field f, const Msg& msg,
                                          size_t idx = 0) {
    return msg.Get<double>(f->proto().number())[idx];
  }

  static y_absl::StatusOr<arc_i32> GetInt32(Field f, const Msg& msg,
                                          size_t idx = 0) {
    return msg.Get<arc_i32>(f->proto().number())[idx];
  }

  static y_absl::StatusOr<arc_ui32> GetUInt32(Field f, const Msg& msg,
                                            size_t idx = 0) {
    return msg.Get<arc_ui32>(f->proto().number())[idx];
  }

  static y_absl::StatusOr<arc_i64> GetInt64(Field f, const Msg& msg,
                                          size_t idx = 0) {
    return msg.Get<arc_i64>(f->proto().number())[idx];
  }

  static y_absl::StatusOr<arc_ui64> GetUInt64(Field f, const Msg& msg,
                                            size_t idx = 0) {
    return msg.Get<arc_ui64>(f->proto().number())[idx];
  }

  static y_absl::StatusOr<bool> GetBool(Field f, const Msg& msg, size_t idx = 0) {
    return msg.Get<Msg::Bool>(f->proto().number())[idx] == Msg::kTrue;
  }

  static y_absl::StatusOr<arc_i32> GetEnumValue(Field f, const Msg& msg,
                                              size_t idx = 0) {
    return msg.Get<arc_i32>(f->proto().number())[idx];
  }

  static y_absl::StatusOr<y_absl::string_view> GetString(Field f,
                                                     TProtoStringType& scratch,
                                                     const Msg& msg,
                                                     size_t idx = 0) {
    return msg.Get<TProtoStringType>(f->proto().number())[idx];
  }

  static y_absl::StatusOr<const Msg*> GetMessage(Field f, const Msg& msg,
                                               size_t idx = 0) {
    return &msg.Get<Msg>(f->proto().number())[idx];
  }

  template <typename F>
  static y_absl::Status WithDecodedMessage(const Desc& desc,
                                         y_absl::string_view data, F body) {
    io::CodedInputStream stream(reinterpret_cast<const uint8_t*>(data.data()),
                                data.size());
    auto unerased = Msg::ParseFromStream(&desc, stream);
    RETURN_IF_ERROR(unerased.status());

    // Explicitly create a const reference, so that we do not accidentally pass
    // a mutable reference to `body`.
    const Msg& ref = *unerased;
    return body(ref);
  }
};
}  // namespace json_internal
}  // namespace protobuf
}  // namespace google

#include "google/protobuf/port_undef.inc"
#endif  // GOOGLE_PROTOBUF_JSON_INTERNAL_UNPARSER_TRAITS_H__
