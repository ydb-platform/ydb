// Copyright 2017 The Abseil Authors.
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

#include "y_absl/strings/str_cat.h"

#include <assert.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <util/generic/string.h>

#include "y_absl/strings/ascii.h"
#include "y_absl/strings/internal/resize_uninitialized.h"
#include "y_absl/strings/numbers.h"
#include "y_absl/strings/string_view.h"

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN

AlphaNum::AlphaNum(Hex hex) {
  static_assert(numbers_internal::kFastToBufferSize >= 32,
                "This function only works when output buffer >= 32 bytes long");
  char* const end = &digits_[numbers_internal::kFastToBufferSize];
  auto real_width =
      y_absl::numbers_internal::FastHexToBufferZeroPad16(hex.value, end - 16);
  if (real_width >= hex.width) {
    piece_ = y_absl::string_view(end - real_width, real_width);
  } else {
    // Pad first 16 chars because FastHexToBufferZeroPad16 pads only to 16 and
    // max pad width can be up to 20.
    std::memset(end - 32, hex.fill, 16);
    // Patch up everything else up to the real_width.
    std::memset(end - real_width - 16, hex.fill, 16);
    piece_ = y_absl::string_view(end - hex.width, hex.width);
  }
}

AlphaNum::AlphaNum(Dec dec) {
  assert(dec.width <= numbers_internal::kFastToBufferSize);
  char* const end = &digits_[numbers_internal::kFastToBufferSize];
  char* const minfill = end - dec.width;
  char* writer = end;
  uint64_t value = dec.value;
  bool neg = dec.neg;
  while (value > 9) {
    *--writer = '0' + (value % 10);
    value /= 10;
  }
  *--writer = '0' + static_cast<char>(value);
  if (neg) *--writer = '-';

  ptrdiff_t fillers = writer - minfill;
  if (fillers > 0) {
    // Tricky: if the fill character is ' ', then it's <fill><+/-><digits>
    // But...: if the fill character is '0', then it's <+/-><fill><digits>
    bool add_sign_again = false;
    if (neg && dec.fill == '0') {  // If filling with '0',
      ++writer;                    // ignore the sign we just added
      add_sign_again = true;       // and re-add the sign later.
    }
    writer -= fillers;
    std::fill_n(writer, fillers, dec.fill);
    if (add_sign_again) *--writer = '-';
  }

  piece_ = y_absl::string_view(writer, static_cast<size_t>(end - writer));
}

// ----------------------------------------------------------------------
// StrCat()
//    This merges the given strings or integers, with no delimiter. This
//    is designed to be the fastest possible way to construct a string out
//    of a mix of raw C strings, string_views, strings, and integer values.
// ----------------------------------------------------------------------

// Append is merely a version of memcpy that returns the address of the byte
// after the area just overwritten.
static char* Append(char* out, const AlphaNum& x) {
  // memcpy is allowed to overwrite arbitrary memory, so doing this after the
  // call would force an extra fetch of x.size().
  char* after = out + x.size();
  if (x.size() != 0) {
    memcpy(out, x.data(), x.size());
  }
  return after;
}

TString StrCat(const AlphaNum& a, const AlphaNum& b) {
  TString result;
  y_absl::strings_internal::STLStringResizeUninitialized(&result,
                                                       a.size() + b.size());
  char* const begin = &result[0];
  char* out = begin;
  out = Append(out, a);
  out = Append(out, b);
  assert(out == begin + result.size());
  return result;
}

TString StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c) {
  TString result;
  strings_internal::STLStringResizeUninitialized(
      &result, a.size() + b.size() + c.size());
  char* const begin = &result[0];
  char* out = begin;
  out = Append(out, a);
  out = Append(out, b);
  out = Append(out, c);
  assert(out == begin + result.size());
  return result;
}

TString StrCat(const AlphaNum& a, const AlphaNum& b, const AlphaNum& c,
                   const AlphaNum& d) {
  TString result;
  strings_internal::STLStringResizeUninitialized(
      &result, a.size() + b.size() + c.size() + d.size());
  char* const begin = &result[0];
  char* out = begin;
  out = Append(out, a);
  out = Append(out, b);
  out = Append(out, c);
  out = Append(out, d);
  assert(out == begin + result.size());
  return result;
}

namespace strings_internal {

// Do not call directly - these are not part of the public API.
TString CatPieces(std::initializer_list<y_absl::string_view> pieces) {
  TString result;
  size_t total_size = 0;
  for (y_absl::string_view piece : pieces) total_size += piece.size();
  strings_internal::STLStringResizeUninitialized(&result, total_size);

  char* const begin = &result[0];
  char* out = begin;
  for (y_absl::string_view piece : pieces) {
    const size_t this_size = piece.size();
    if (this_size != 0) {
      memcpy(out, piece.data(), this_size);
      out += this_size;
    }
  }
  assert(out == begin + result.size());
  return result;
}

// It's possible to call StrAppend with an y_absl::string_view that is itself a
// fragment of the string we're appending to.  However the results of this are
// random. Therefore, check for this in debug mode.  Use unsigned math so we
// only have to do one comparison. Note, there's an exception case: appending an
// empty string is always allowed.
#define ASSERT_NO_OVERLAP(dest, src) \
  assert(((src).size() == 0) ||      \
         (uintptr_t((src).data() - (dest).data()) > uintptr_t((dest).size())))

void AppendPieces(TString* dest,
                  std::initializer_list<y_absl::string_view> pieces) {
  size_t old_size = dest->size();
  size_t total_size = old_size;
  for (y_absl::string_view piece : pieces) {
    ASSERT_NO_OVERLAP(*dest, piece);
    total_size += piece.size();
  }
  strings_internal::STLStringResizeUninitializedAmortized(dest, total_size);

  char* const begin = &(*dest)[0];
  char* out = begin + old_size;
  for (y_absl::string_view piece : pieces) {
    const size_t this_size = piece.size();
    if (this_size != 0) {
      memcpy(out, piece.data(), this_size);
      out += this_size;
    }
  }
  assert(out == begin + dest->size());
}

}  // namespace strings_internal

void StrAppend(TString* dest, const AlphaNum& a) {
  ASSERT_NO_OVERLAP(*dest, a);
  dest->append(a.data(), a.size());
}

void StrAppend(TString* dest, const AlphaNum& a, const AlphaNum& b) {
  ASSERT_NO_OVERLAP(*dest, a);
  ASSERT_NO_OVERLAP(*dest, b);
  TString::size_type old_size = dest->size();
  strings_internal::STLStringResizeUninitializedAmortized(
      dest, old_size + a.size() + b.size());
  char* const begin = &(*dest)[0];
  char* out = begin + old_size;
  out = Append(out, a);
  out = Append(out, b);
  assert(out == begin + dest->size());
}

void StrAppend(TString* dest, const AlphaNum& a, const AlphaNum& b,
               const AlphaNum& c) {
  ASSERT_NO_OVERLAP(*dest, a);
  ASSERT_NO_OVERLAP(*dest, b);
  ASSERT_NO_OVERLAP(*dest, c);
  TString::size_type old_size = dest->size();
  strings_internal::STLStringResizeUninitializedAmortized(
      dest, old_size + a.size() + b.size() + c.size());
  char* const begin = &(*dest)[0];
  char* out = begin + old_size;
  out = Append(out, a);
  out = Append(out, b);
  out = Append(out, c);
  assert(out == begin + dest->size());
}

void StrAppend(TString* dest, const AlphaNum& a, const AlphaNum& b,
               const AlphaNum& c, const AlphaNum& d) {
  ASSERT_NO_OVERLAP(*dest, a);
  ASSERT_NO_OVERLAP(*dest, b);
  ASSERT_NO_OVERLAP(*dest, c);
  ASSERT_NO_OVERLAP(*dest, d);
  TString::size_type old_size = dest->size();
  strings_internal::STLStringResizeUninitializedAmortized(
      dest, old_size + a.size() + b.size() + c.size() + d.size());
  char* const begin = &(*dest)[0];
  char* out = begin + old_size;
  out = Append(out, a);
  out = Append(out, b);
  out = Append(out, c);
  out = Append(out, d);
  assert(out == begin + dest->size());
}

Y_ABSL_NAMESPACE_END
}  // namespace y_absl
