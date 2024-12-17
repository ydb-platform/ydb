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

// Utility functions to convert between protobuf binary format and proto3 JSON
// format.
#ifndef GOOGLE_PROTOBUF_JSON_JSON_H__
#define GOOGLE_PROTOBUF_JSON_JSON_H__

#include <string>

#include "y_absl/status/status.h"
#include "y_absl/strings/string_view.h"
#include "google/protobuf/message.h"
#include "google/protobuf/util/type_resolver.h"

// Must be included last.
#include "google/protobuf/port_def.inc"

namespace google {
namespace protobuf {
#ifdef USE_DEPRECATED_NAMESPACE
namespace util {
#else
namespace json {
#endif
struct ParseOptions {
  // Whether to ignore unknown JSON fields during parsing
  bool ignore_unknown_fields = false;

  // If true, when a lowercase enum value fails to parse, try convert it to
  // UPPER_CASE and see if it matches a valid enum.
  // WARNING: This option exists only to preserve legacy behavior. Avoid using
  // this option. If your enum needs to support different casing, consider using
  // allow_alias instead.
  bool case_insensitive_enum_parsing = false;

  ParseOptions()
      : ignore_unknown_fields(false), case_insensitive_enum_parsing(false) {}
};

struct PrintOptions {
  // Whether to add spaces, line breaks and indentation to make the JSON output
  // easy to read.
  bool add_whitespace = false;
  // Whether to always print primitive fields. By default proto3 primitive
  // fields with default values will be omitted in JSON output. For example, an
  // int32 field set to 0 will be omitted. Set this flag to true will override
  // the default behavior and print primitive fields regardless of their values.
  bool always_print_primitive_fields = false;
  // Whether to always print enums as ints. By default they are rendered as
  // strings.
  bool always_print_enums_as_ints = false;
  // Whether to preserve proto field names
  bool preserve_proto_field_names = false;

  PrintOptions()
      : add_whitespace(false),
        always_print_primitive_fields(false),
        always_print_enums_as_ints(false),
        preserve_proto_field_names(false) {}
};

#ifdef USE_DEPRECATED_NAMESPACE
using JsonParseOptions = ParseOptions;
using JsonPrintOptions = PrintOptions;
using JsonOptions = PrintOptions;
#endif

// Converts from protobuf message to JSON and appends it to |output|. This is a
// simple wrapper of BinaryToJsonString(). It will use the DescriptorPool of the
// passed-in message to resolve Any types.
//
// Please note that non-OK statuses are not a stable output of this API and
// subject to change without notice.
PROTOBUF_EXPORT y_absl::Status MessageToJsonString(const Message& message,
                                                 TProtoStringType* output,
                                                 const PrintOptions& options);

inline y_absl::Status MessageToJsonString(const Message& message,
                                        TProtoStringType* output) {
  return MessageToJsonString(message, output, PrintOptions());
}

// Converts from JSON to protobuf message. This is a simple wrapper of
// JsonStringToBinary(). It will use the DescriptorPool of the passed-in
// message to resolve Any types.
//
// Please note that non-OK statuses are not a stable output of this API and
// subject to change without notice.
PROTOBUF_EXPORT y_absl::Status JsonStringToMessage(y_absl::string_view input,
                                                 Message* message,
                                                 const ParseOptions& options);

inline y_absl::Status JsonStringToMessage(y_absl::string_view input,
                                        Message* message) {
  return JsonStringToMessage(input, message, ParseOptions());
}

// Converts protobuf binary data to JSON.
// The conversion will fail if:
//   1. TypeResolver fails to resolve a type.
//   2. input is not valid protobuf wire format, or conflicts with the type
//      information returned by TypeResolver.
// Note that unknown fields will be discarded silently.
//
// Please note that non-OK statuses are not a stable output of this API and
// subject to change without notice.
PROTOBUF_EXPORT y_absl::Status BinaryToJsonStream(
    google::protobuf::util::TypeResolver* resolver, const TProtoStringType& type_url,
    io::ZeroCopyInputStream* binary_input,
    io::ZeroCopyOutputStream* json_output, const PrintOptions& options);

inline y_absl::Status BinaryToJsonStream(google::protobuf::util::TypeResolver* resolver,
                                       const TProtoStringType& type_url,
                                       io::ZeroCopyInputStream* binary_input,
                                       io::ZeroCopyOutputStream* json_output) {
  return BinaryToJsonStream(resolver, type_url, binary_input, json_output,
                            PrintOptions());
}

PROTOBUF_EXPORT y_absl::Status BinaryToJsonString(
    google::protobuf::util::TypeResolver* resolver, const TProtoStringType& type_url,
    const TProtoStringType& binary_input, TProtoStringType* json_output,
    const PrintOptions& options);

inline y_absl::Status BinaryToJsonString(google::protobuf::util::TypeResolver* resolver,
                                       const TProtoStringType& type_url,
                                       const TProtoStringType& binary_input,
                                       TProtoStringType* json_output) {
  return BinaryToJsonString(resolver, type_url, binary_input, json_output,
                            PrintOptions());
}

// Converts JSON data to protobuf binary format.
// The conversion will fail if:
//   1. TypeResolver fails to resolve a type.
//   2. input is not valid JSON format, or conflicts with the type
//      information returned by TypeResolver.
//
// Please note that non-OK statuses are not a stable output of this API and
// subject to change without notice.
PROTOBUF_EXPORT y_absl::Status JsonToBinaryStream(
    google::protobuf::util::TypeResolver* resolver, const TProtoStringType& type_url,
    io::ZeroCopyInputStream* json_input,
    io::ZeroCopyOutputStream* binary_output, const ParseOptions& options);

inline y_absl::Status JsonToBinaryStream(
    google::protobuf::util::TypeResolver* resolver, const TProtoStringType& type_url,
    io::ZeroCopyInputStream* json_input,
    io::ZeroCopyOutputStream* binary_output) {
  return JsonToBinaryStream(resolver, type_url, json_input, binary_output,
                            ParseOptions());
}

PROTOBUF_EXPORT y_absl::Status JsonToBinaryString(
    google::protobuf::util::TypeResolver* resolver, const TProtoStringType& type_url,
    y_absl::string_view json_input, TProtoStringType* binary_output,
    const ParseOptions& options);

inline y_absl::Status JsonToBinaryString(google::protobuf::util::TypeResolver* resolver,
                                       const TProtoStringType& type_url,
                                       y_absl::string_view json_input,
                                       TProtoStringType* binary_output) {
  return JsonToBinaryString(resolver, type_url, json_input, binary_output,
                            ParseOptions());
}
}  // namespace json
}  // namespace protobuf
}  // namespace google

#include "google/protobuf/port_undef.inc"

#endif  // GOOGLE_PROTOBUF_JSON_JSON_H__
