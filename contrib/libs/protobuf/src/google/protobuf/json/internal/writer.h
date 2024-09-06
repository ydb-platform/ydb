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

#ifndef GOOGLE_PROTOBUF_JSON_INTERNAL_WRITER_H__
#define GOOGLE_PROTOBUF_JSON_INTERNAL_WRITER_H__

#include <cfloat>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <limits>
#include <ostream>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "y_absl/strings/str_format.h"
#include "y_absl/strings/string_view.h"
#include "google/protobuf/io/strtod.h"
#include "google/protobuf/io/zero_copy_sink.h"
#include "google/protobuf/io/zero_copy_stream.h"
#include "google/protobuf/stubs/status_macros.h"

// Must be included last.
#include "google/protobuf/port_def.inc"

namespace google {
namespace protobuf {
namespace json_internal {
struct WriterOptions {
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
  // The original parser used by json_util2 accepted a number of non-standard
  // options. Setting this flag enables them.
  //
  // What those extensions were is explicitly not documented, beyond what exists
  // in the unit tests; we intend to remove this setting eventually. See
  // b/234868512.
  bool allow_legacy_syntax = false;
};

template <typename Tuple, typename F, size_t... i>
void EachInner(const Tuple& value, F f, std::index_sequence<i...>) {
  int ignored[] = {
      (f(std::get<i>(value)), 0)...};  // NOLINT(readability/braces)
  (void)ignored;
}

// Executes f on each element of value.
template <typename Tuple, typename F>
void Each(const Tuple& value, F f) {
  EachInner(value, f,
            std::make_index_sequence<std::tuple_size<Tuple>::value>());
}

// See JsonWriter::Write().
template <typename... T>
struct Quoted {
  std::tuple<T...> value;
};

// Because this is not C++17 yet, we cannot add a deduction guide.
template <typename... T>
static Quoted<T...> MakeQuoted(T... t) {
  return Quoted<T...>{std::make_tuple(t...)};
}

class JsonWriter {
 public:
  JsonWriter(io::ZeroCopyOutputStream* out, WriterOptions options)
      : sink_(out), options_(options) {}

  const WriterOptions& options() const { return options_; }

  void Push() { ++indent_; }
  void Pop() { --indent_; }

  // The many overloads of Write() will write a value to the underlying stream.
  // Some values may want to be quoted; the Quoted<> type will automatically add
  // quotes and escape sequences.
  //
  // Note that Write() is not implemented for 64-bit integers, since they
  // cannot be crisply represented without quotes; use MakeQuoted for that.

  void Write(y_absl::string_view str) { sink_.Append(str.data(), str.size()); }

  void Write(char c) { sink_.Append(&c, 1); }

  // The precision on this and the following function are completely made-up,
  // in an attempt to match the behavior of the ESF parser.
  void Write(double val) {
    if (!MaybeWriteSpecialFp(val)) {
      Write(io::SimpleDtoa(val));
    }
  }

  void Write(float val) {
    if (!MaybeWriteSpecialFp(val)) {
      Write(io::SimpleFtoa(val));
    }
  }

  void Write(arc_i32 val) {
    char buf[22];
    int len = y_absl::SNPrintF(buf, sizeof(buf), "%d", val);
    y_absl::string_view view(buf, static_cast<size_t>(len));
    Write(view);
  }

  void Write(arc_ui32 val) {
    char buf[22];
    int len = y_absl::SNPrintF(buf, sizeof(buf), "%d", val);
    y_absl::string_view view(buf, static_cast<size_t>(len));
    Write(view);
  }

  void Write(arc_i64) = delete;
  void Write(arc_ui64) = delete;

  template <typename... Ts>
  void Write(Quoted<Ts...> val) {
    Write('"');
    Each(val.value, [this](auto x) { this->WriteQuoted(x); });
    Write('"');
  }

  template <typename... Ts>
  auto Write(Ts... args) ->
      // This bit of SFINAE avoids this function being called with one argument,
      // so the other overloads of Write() can be picked up instead.
      typename std::enable_if<sizeof...(Ts) != 1, void>::type {
    Each(std::make_tuple(args...), [this](auto x) { this->Write(x); });
  }

  void Whitespace(y_absl::string_view ws) {
    if (options_.add_whitespace) {
      Write(ws);
    }
  }

  void NewLine() {
    Whitespace("\n");
    for (int i = 0; i < indent_; ++i) {
      Whitespace(" ");
    }
  }

  void WriteComma(bool& is_first) {
    if (is_first) {
      is_first = false;
      return;
    }
    Write(",");
  }

  void WriteBase64(y_absl::string_view str);

  // Returns a buffer that can be re-used throughout a writing session as
  // variable-length scratch space.
  TProtoStringType& ScratchBuf() { return scratch_buf_; }

 private:
  template <typename T>
  void WriteQuoted(T val) {
    Write(val);
  }

  void WriteQuoted(y_absl::string_view val) { WriteEscapedUtf8(val); }

  void WriteQuoted(arc_i64 val) {
    char buf[22];
    int len = y_absl::SNPrintF(buf, sizeof(buf), "%d", val);
    y_absl::string_view view(buf, static_cast<size_t>(len));
    Write(view);
  }

  void WriteQuoted(arc_ui64 val) {
    char buf[22];
    int len = y_absl::SNPrintF(buf, sizeof(buf), "%d", val);
    y_absl::string_view view(buf, static_cast<size_t>(len));
    Write(view);
  }

  // Tries to write a non-finite double if necessary; returns false if
  // nothing was written.
  bool MaybeWriteSpecialFp(double val);

  void WriteEscapedUtf8(y_absl::string_view str);
  void WriteUEscape(uint16_t val);

  io::zc_sink_internal::ZeroCopyStreamByteSink sink_;
  WriterOptions options_;
  int indent_ = 0;

  TProtoStringType scratch_buf_;
};
}  // namespace json_internal
}  // namespace protobuf
}  // namespace google

#include "google/protobuf/port_undef.inc"
#endif  // GOOGLE_PROTOBUF_JSON_INTERNAL_WRITER_H__
