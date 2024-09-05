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

#include "google/protobuf/json/internal/lexer.h"

#include <sys/types.h>

#include <atomic>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <limits>
#include <ostream>
#include <string>
#include <utility>

#include "y_absl/algorithm/container.h"
#include "y_absl/log/absl_check.h"
#include "y_absl/numeric/bits.h"
#include "y_absl/status/status.h"
#include "y_absl/status/statusor.h"
#include "y_absl/strings/ascii.h"
#include "y_absl/strings/numbers.h"
#include "y_absl/strings/str_cat.h"
#include "y_absl/strings/str_format.h"
#include "y_absl/strings/string_view.h"
#include "google/protobuf/stubs/status_macros.h"

// Must be included last.
#include "google/protobuf/port_def.inc"

namespace google {
namespace protobuf {
namespace json_internal {
namespace {
// Randomly inserts bonus whitespace of a few different kinds into a string.
//
// This utility is intended to make error messages hostile to machine
// interpretation as a Hyrum's Law countermeasure, without potentially confusing
// human readers.
void HardenAgainstHyrumsLaw(y_absl::string_view to_obfuscate, TProtoStringType& out) {
  // Get some simple randomness from ASLR, which is enabled in most
  // environments. Our goal is to be annoying, not secure.
  static const void* const kAslrSeed = &kAslrSeed;
  // Per-call randomness from a relaxed atomic.
  static std::atomic<uintptr_t> kCounterSeed{0};

  constexpr arc_ui64 kA = 0x5851f42d4c957f2dull;
  constexpr arc_ui64 kB = 0x14057b7ef767814full;

  arc_ui64 state = y_absl::bit_cast<uintptr_t>(kAslrSeed) + kB +
                   kCounterSeed.fetch_add(1, std::memory_order_relaxed);
  auto rng = [&state, &kA, &kB] {
    state = state * kA + kB;
    return y_absl::rotr(static_cast<arc_ui32>(((state >> 18) ^ state) >> 27),
                      state >> 59);
  };
  (void)rng();  // Advance state once.

  out.reserve(to_obfuscate.size() + y_absl::c_count(to_obfuscate, ' '));
  for (char c : to_obfuscate) {
    out.push_back(c);
    if (c != ' ' || rng() % 3 != 0) {
      continue;
    }

    size_t count = rng() % 2 + 1;
    for (size_t i = 0; i < count; ++i) {
      out.push_back(' ');
    }
  }
}
}  // namespace

constexpr size_t ParseOptions::kDefaultDepth;

y_absl::Status JsonLocation::Invalid(y_absl::string_view message,
                                   SourceLocation sl) const {
  // NOTE: we intentionally do not harden the "invalid JSON" part, so that
  // people have a hope of grepping for it in logs. That part is easy to
  // commit to, as stability goes.
  //
  // This copies the error twice. Because this is the "unhappy" path, this
  // function is cold and can afford the waste.
  TProtoStringType status_message = "invalid JSON";
  TProtoStringType to_obfuscate;
  if (path != nullptr) {
    y_absl::StrAppend(&to_obfuscate, " in ");
    path->Describe(to_obfuscate);
    to_obfuscate.push_back(',');
  }
  y_absl::StrAppendFormat(&to_obfuscate, " near %zu:%zu (offset %zu): %s",
                        line + 1, col + 1, offset, message);
  HardenAgainstHyrumsLaw(to_obfuscate, status_message);

  return y_absl::InvalidArgumentError(std::move(status_message));
}

y_absl::StatusOr<JsonLexer::Kind> JsonLexer::PeekKind() {
  RETURN_IF_ERROR(SkipToToken());
  char c = stream_.PeekChar();
  switch (c) {
    case '{':
      return JsonLexer::kObj;
    case '[':
      return JsonLexer::kArr;
    case '"':
    case '\'':
      return JsonLexer::kStr;
    case '-':
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
      return JsonLexer::kNum;
    case 't':
      return JsonLexer::kTrue;
    case 'f':
      return JsonLexer::kFalse;
    case 'n':
      return JsonLexer::kNull;
    default:
      return Invalid(y_absl::StrFormat("unexpected character: '%c'", c));
  }
}

y_absl::Status JsonLexer::SkipValue() {
  y_absl::StatusOr<Kind> kind = PeekKind();
  RETURN_IF_ERROR(kind.status());

  switch (*kind) {
    case JsonLexer::kObj:
      return VisitObject(
          [this](LocationWith<MaybeOwnedString>&) { return SkipValue(); });
    case JsonLexer::kArr:
      return VisitArray([this] { return SkipValue(); });
    case JsonLexer::kStr:
      return ParseUtf8().status();
    case JsonLexer::kNum:
      return ParseNumber().status();
    case JsonLexer::kTrue:
      return Expect("true");
    case JsonLexer::kFalse:
      return Expect("false");
    case JsonLexer::kNull:
      return Expect("null");
    default:
      break;
  }
  // Some compilers seem to fail to realize this is a basic block
  // terminator and incorrectly believe this function is missing
  // a return.
  Y_ABSL_CHECK(false) << "unreachable";
  return y_absl::OkStatus();
}

y_absl::StatusOr<uint16_t> JsonLexer::ParseU16HexCodepoint() {
  y_absl::StatusOr<LocationWith<MaybeOwnedString>> escape = Take(4);
  RETURN_IF_ERROR(escape.status());

  uint16_t u16 = 0;
  for (char c : escape->value.AsView()) {
    if (c >= '0' && c <= '9') {
      c -= '0';
    } else if (c >= 'a' && c <= 'f') {
      c = c - 'a' + 10;
    } else if (c >= 'A' && c <= 'F') {
      c = c - 'A' + 10;
    } else {
      return Invalid("invalid Unicode escape");
    }
    u16 <<= 4;
    u16 |= c;
  }

  return u16;
}

y_absl::Status JsonLexer::SkipToToken() {
  while (true) {
    RETURN_IF_ERROR(stream_.BufferAtLeast(1).status());
    switch (stream_.PeekChar()) {
      case '\n':
        RETURN_IF_ERROR(Advance(1));
        ++json_loc_.line;
        json_loc_.col = 0;
        break;
      case '\r':
      case '\t':
      case ' ':
        RETURN_IF_ERROR(Advance(1));
        break;
      default:
        return y_absl::OkStatus();
    }
  }
}

y_absl::StatusOr<LocationWith<MaybeOwnedString>> JsonLexer::ParseRawNumber() {
  RETURN_IF_ERROR(SkipToToken());

  enum { kInt, kFraction, kExponent } state = kInt;
  char prev_var = 0;
  auto number = TakeWhile([state, prev_var](size_t index, char c) mutable {
    char prev = prev_var;
    prev_var = c;
    if (y_absl::ascii_isdigit(c)) {
      return true;
    }

    bool last_was_int = y_absl::ascii_isdigit(prev);
    // These checks handle transitions between the integer, fractional, and
    // exponent part of a number. This will cut off at the first syntax error.
    // Because all numbers must be followed by `,`, `]`, or `}`, we can let
    // that catch what's left behind.
    if (state == kInt && c == '-') {
      return !last_was_int;
    }
    if (state == kInt && last_was_int && c == '.') {
      state = kFraction;
      return true;
    }
    if (state != kExponent && last_was_int && (c == 'e' || c == 'E')) {
      state = kExponent;
      return true;
    }
    if ((prev == 'e' || prev == 'E') && (c == '-' || c == '+')) {
      return true;
    }

    return false;
  });

  RETURN_IF_ERROR(number.status());
  y_absl::string_view number_text = number->value.AsView();

  if (number_text.empty() || number_text == "-") {
    return number->loc.Invalid("expected a number");
  }

  auto without_minus =
      number_text[0] == '-' ? number_text.substr(1) : number_text;
  if (without_minus.size() > 1 && without_minus[0] == '0' &&
      y_absl::ascii_isdigit(without_minus[1])) {
    return number->loc.Invalid("number cannot have extraneous leading zero");
  }

  if (number_text.back() == '.') {
    return number->loc.Invalid("number cannot have trailing period");
  }

  double d;
  if (!y_absl::SimpleAtod(number_text, &d) || !std::isfinite(d)) {
    return number->loc.Invalid(
        y_absl::StrFormat("invalid number: '%s'", number_text));
  }

  // Find the next token, to make sure we didn't leave something behind we
  // shouldn't have.
  if (!stream_.AtEof()) {
    RETURN_IF_ERROR(SkipToToken());
    switch (stream_.PeekChar()) {
      case ',':
      case ']':
      case '}':
        break;
      default:
        return Invalid(
            y_absl::StrFormat("unexpected character: '%c'", stream_.PeekChar()));
    }
  }

  return number;
}

y_absl::StatusOr<LocationWith<double>> JsonLexer::ParseNumber() {
  auto number = ParseRawNumber();
  RETURN_IF_ERROR(number.status());

  double d;
  if (!y_absl::SimpleAtod(number->value.AsView(), &d) || !std::isfinite(d)) {
    return number->loc.Invalid(
        y_absl::StrFormat("invalid number: '%s'", number->value.AsView()));
  }

  return LocationWith<double>{d, number->loc};
}

y_absl::StatusOr<size_t> JsonLexer::ParseUnicodeEscape(char out_utf8[4]) {
  auto hex = ParseU16HexCodepoint();
  RETURN_IF_ERROR(hex.status());

  arc_ui32 rune = *hex;
  if (rune >= 0xd800 && rune <= 0xdbff) {
    // Surrogate pair: two 16-bit codepoints become a 32-bit codepoint.
    arc_ui32 high = rune;

    RETURN_IF_ERROR(Expect("\\u"));
    auto hex = ParseU16HexCodepoint();
    RETURN_IF_ERROR(hex.status());

    arc_ui32 low = *hex;
    if (low < 0xdc00 || low > 0xdfff) {
      return Invalid("invalid low surrogate");
    }

    rune = (high & 0x3ff) << 10;
    rune |= (low & 0x3ff);
    rune += 0x10000;
  } else if (rune >= 0xdc00 && rune <= 0xdfff) {
    return Invalid("unpaired low surrogate");
  }

  // Write as UTF-8.
  if (rune <= 0x7f) {
    out_utf8[0] = rune;
    return 1;
  } else if (rune <= 0x07ff) {
    out_utf8[0] = ((rune >> 6) & 0x1f) | 0xc0;
    out_utf8[1] = ((rune >> 0) & 0x3f) | 0x80;
    return 2;
  } else if (rune <= 0xffff) {
    out_utf8[0] = ((rune >> 12) & 0x0f) | 0xe0;
    out_utf8[1] = ((rune >> 6) & 0x3f) | 0x80;
    out_utf8[2] = ((rune >> 0) & 0x3f) | 0x80;
    return 3;
  } else if (rune < 0x10ffff) {
    out_utf8[0] = ((rune >> 18) & 0x07) | 0xF0;
    out_utf8[1] = ((rune >> 12) & 0x3f) | 0x80;
    out_utf8[2] = ((rune >> 6) & 0x3f) | 0x80;
    out_utf8[3] = ((rune >> 0) & 0x3f) | 0x80;
    return 4;
  } else {
    return Invalid("invalid codepoint");
  }
}

static char ParseSimpleEscape(char c, bool allow_legacy_syntax) {
  switch (c) {
    case '"':
      return '"';
    case '\\':
      return '\\';
    case '/':
      return '/';
    case 'b':
      return '\b';
    case 'f':
      return '\f';
    case 'n':
      return '\n';
    case 'r':
      return '\r';
    case 't':
      return '\t';
    case '\'':
      if (allow_legacy_syntax) {
        return '\'';
      }
      Y_ABSL_FALLTHROUGH_INTENDED;
    default:
      return 0;
  }
}

y_absl::StatusOr<LocationWith<MaybeOwnedString>> JsonLexer::ParseUtf8() {
  RETURN_IF_ERROR(SkipToToken());
  // This is a non-standard extension accepted by the ESF parser that we will
  // need to accept for backwards-compat.
  bool is_single_quote = stream_.PeekChar() == '\'';
  if (!options_.allow_legacy_syntax && is_single_quote) {
    return Invalid("expected '\"'");
  }

  JsonLocation loc = json_loc_;
  RETURN_IF_ERROR(Expect(is_single_quote ? "'" : "\""));

  // on_heap is empty if we do not need to heap-allocate the string.
  TProtoStringType on_heap;
  LocationWith<Mark> mark = BeginMark();
  while (true) {
    RETURN_IF_ERROR(stream_.BufferAtLeast(1).status());

    char c = stream_.PeekChar();
    RETURN_IF_ERROR(Advance(1));
    switch (c) {
      case '"':
      case '\'': {
        if (c != (is_single_quote ? '\'' : '"')) {
          goto normal_character;
        }

        if (!on_heap.empty()) {
          return LocationWith<MaybeOwnedString>{
              MaybeOwnedString(std::move(on_heap)), loc};
        }
        // NOTE: the 1 below clips off the " from the end of the string.
        return LocationWith<MaybeOwnedString>{mark.value.UpToUnread(1), loc};
      }
      case '\\': {
        if (on_heap.empty()) {
          // The 1 skips over the `\`.
          on_heap = TProtoStringType(mark.value.UpToUnread(1).AsView());
          // Clang-tidy incorrectly notes this as being moved-from multiple
          // times, but it can only occur in one loop iteration. The mark is
          // destroyed only if we need to handle an escape when on_heap is
          // empty. Because this branch unconditionally pushes to on_heap, this
          // condition can never be reached in any iteration that follows it.
          // This, at most one move every actually occurs.
          std::move(mark).value.Discard();
        }
        RETURN_IF_ERROR(stream_.BufferAtLeast(1).status());

        char c = stream_.PeekChar();
        RETURN_IF_ERROR(Advance(1));
        if (c == 'u' || (c == 'U' && options_.allow_legacy_syntax)) {
          // Ensure there is actual space to scribble the UTF-8 onto.
          on_heap.resize(on_heap.size() + 4);
          auto written = ParseUnicodeEscape(&on_heap[on_heap.size() - 4]);
          RETURN_IF_ERROR(written.status());
          on_heap.resize(on_heap.size() - 4 + *written);
        } else {
          char escape = ParseSimpleEscape(c, options_.allow_legacy_syntax);
          if (escape == 0) {
            return Invalid(y_absl::StrFormat("invalid escape char: '%c'", c));
          }
          on_heap.push_back(escape);
        }
        break;
      }
      normal_character:
      default: {
        uint8_t uc = static_cast<uint8_t>(c);
        // If people have newlines in their strings, that's their problem; it
        // is too difficult to support correctly in our location tracking, and
        // is out of spec, so users will get slightly wrong locations in errors.
        if ((uc < 0x20 || uc == 0xff) && !options_.allow_legacy_syntax) {
          return Invalid(y_absl::StrFormat(
              "invalid control character 0x%02x in string", uc));
        }

        // Verify this is valid UTF-8. UTF-8 is a varint encoding satisfying
        // one of the following (big-endian) patterns:
        //
        // 0b0xxxxxxx
        // 0b110xxxxx'10xxxxxx
        // 0b1110xxxx'10xxxxxx'10xxxxxx
        // 0b11110xxx'10xxxxxx'10xxxxxx'10xxxxxx
        //
        // We don't need to decode it; just validate it.
        size_t lookahead = 0;
        switch (y_absl::countl_one(uc)) {
          case 0:
            break;
          case 2:
            lookahead = 1;
            break;
          case 3:
            lookahead = 2;
            break;
          case 4:
            lookahead = 3;
            break;
          default:
            return Invalid("invalid UTF-8 in string");
        }

        if (!on_heap.empty()) {
          on_heap.push_back(c);
        }
        for (int i = 0; i < lookahead; ++i) {
          RETURN_IF_ERROR(stream_.BufferAtLeast(1).status());
          uint8_t uc = static_cast<uint8_t>(stream_.PeekChar());
          if ((uc >> 6) != 2) {
            return Invalid("invalid UTF-8 in string");
          }
          if (!on_heap.empty()) {
            on_heap.push_back(stream_.PeekChar());
          }
          RETURN_IF_ERROR(Advance(1));
        }
        break;
      }
    }
  }

  return Invalid("EOF inside string");
}

y_absl::StatusOr<LocationWith<MaybeOwnedString>> JsonLexer::ParseBareWord() {
  RETURN_IF_ERROR(SkipToToken());
  auto ident = TakeWhile(
      [](size_t, char c) { return c == '_' || y_absl::ascii_isalnum(c); });
  RETURN_IF_ERROR(ident.status());
  y_absl::string_view text = ident->value.AsView();

  if (text.empty() || y_absl::ascii_isdigit(text[0]) || text == "null" ||
      text == "true" || text == "false") {
    return ident->loc.Invalid("expected bare word");
  }
  return ident;
}

}  // namespace json_internal
}  // namespace protobuf
}  // namespace google
