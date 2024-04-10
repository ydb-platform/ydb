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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <util/generic/string.h>
#include <type_traits>

#include "y_absl/base/config.h"
#include "y_absl/base/nullability.h"
#include "y_absl/strings/internal/resize_uninitialized.h"
#include "y_absl/strings/numbers.h"
#include "y_absl/strings/string_view.h"

namespace y_absl {
Y_ABSL_NAMESPACE_BEGIN


// ----------------------------------------------------------------------
// StrCat()
//    This merges the given strings or integers, with no delimiter. This
//    is designed to be the fastest possible way to construct a string out
//    of a mix of raw C strings, string_views, strings, and integer values.
// ----------------------------------------------------------------------

namespace {
// Append is merely a version of memcpy that returns the address of the byte
// after the area just overwritten.
y_absl::Nonnull<char*> Append(y_absl::Nonnull<char*> out, const AlphaNum& x) {
  // memcpy is allowed to overwrite arbitrary memory, so doing this after the
  // call would force an extra fetch of x.size().
  char* after = out + x.size();
  if (x.size() != 0) {
    memcpy(out, x.data(), x.size());
  }
  return after;
}

}  // namespace

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
void STLStringAppendUninitializedAmortized(TString* dest,
                                           size_t to_append) {
  strings_internal::AppendUninitializedTraits<TString>::Append(dest,
                                                                   to_append);
}

template <typename Integer>
std::enable_if_t<std::is_integral<Integer>::value, TString> IntegerToString(
    Integer i) {
  TString str;
  const auto /* either bool or std::false_type */ is_negative =
      y_absl::numbers_internal::IsNegative(i);
  const uint32_t digits = y_absl::numbers_internal::Base10Digits(
      y_absl::numbers_internal::UnsignedAbsoluteValue(i));
  y_absl::strings_internal::STLStringResizeUninitialized(
      &str, digits + static_cast<uint32_t>(is_negative));
  y_absl::numbers_internal::FastIntToBufferBackward(i, &str[str.size()], digits);
  return str;
}

template <>
TString IntegerToString(long i) {  // NOLINT
  if (sizeof(i) <= sizeof(int)) {
    return IntegerToString(static_cast<int>(i));
  } else {
    return IntegerToString(static_cast<long long>(i));  // NOLINT
  }
}

template <>
TString IntegerToString(unsigned long i) {  // NOLINT
  if (sizeof(i) <= sizeof(unsigned int)) {
    return IntegerToString(static_cast<unsigned int>(i));
  } else {
    return IntegerToString(static_cast<unsigned long long>(i));  // NOLINT
  }
}

template <typename Float>
std::enable_if_t<std::is_floating_point<Float>::value, TString>
FloatToString(Float f) {
  TString result;
  strings_internal::STLStringResizeUninitialized(
      &result, numbers_internal::kSixDigitsToBufferSize);
  char* start = &result[0];
  result.erase(numbers_internal::SixDigitsToBuffer(f, start));
  return result;
}

TString SingleArgStrCat(int x) { return IntegerToString(x); }
TString SingleArgStrCat(unsigned int x) { return IntegerToString(x); }
// NOLINTNEXTLINE
TString SingleArgStrCat(long x) { return IntegerToString(x); }
// NOLINTNEXTLINE
TString SingleArgStrCat(unsigned long x) { return IntegerToString(x); }
// NOLINTNEXTLINE
TString SingleArgStrCat(long long x) { return IntegerToString(x); }
// NOLINTNEXTLINE
TString SingleArgStrCat(unsigned long long x) { return IntegerToString(x); }
TString SingleArgStrCat(float x) { return FloatToString(x); }
TString SingleArgStrCat(double x) { return FloatToString(x); }

template <class Integer>
std::enable_if_t<std::is_integral<Integer>::value, void> AppendIntegerToString(
    TString& str, Integer i) {
  const auto /* either bool or std::false_type */ is_negative =
      y_absl::numbers_internal::IsNegative(i);
  const uint32_t digits = y_absl::numbers_internal::Base10Digits(
      y_absl::numbers_internal::UnsignedAbsoluteValue(i));
  y_absl::strings_internal::STLStringAppendUninitializedAmortized(
      &str, digits + static_cast<uint32_t>(is_negative));
  y_absl::numbers_internal::FastIntToBufferBackward(i, &str[str.size()], digits);
}

template <>
void AppendIntegerToString(TString& str, long i) {  // NOLINT
  if (sizeof(i) <= sizeof(int)) {
    return AppendIntegerToString(str, static_cast<int>(i));
  } else {
    return AppendIntegerToString(str, static_cast<long long>(i));  // NOLINT
  }
}

template <>
void AppendIntegerToString(TString& str,
                           unsigned long i) {  // NOLINT
  if (sizeof(i) <= sizeof(unsigned int)) {
    return AppendIntegerToString(str, static_cast<unsigned int>(i));
  } else {
    return AppendIntegerToString(str,
                                 static_cast<unsigned long long>(i));  // NOLINT
  }
}

// `SingleArgStrAppend` overloads are defined here for the same reasons as with
// `SingleArgStrCat` above.
void SingleArgStrAppend(TString& str, int x) {
  return AppendIntegerToString(str, x);
}

void SingleArgStrAppend(TString& str, unsigned int x) {
  return AppendIntegerToString(str, x);
}

// NOLINTNEXTLINE
void SingleArgStrAppend(TString& str, long x) {
  return AppendIntegerToString(str, x);
}

// NOLINTNEXTLINE
void SingleArgStrAppend(TString& str, unsigned long x) {
  return AppendIntegerToString(str, x);
}

// NOLINTNEXTLINE
void SingleArgStrAppend(TString& str, long long x) {
  return AppendIntegerToString(str, x);
}

// NOLINTNEXTLINE
void SingleArgStrAppend(TString& str, unsigned long long x) {
  return AppendIntegerToString(str, x);
}

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

void AppendPieces(y_absl::Nonnull<TString*> dest,
                  std::initializer_list<y_absl::string_view> pieces) {
  size_t old_size = dest->size();
  size_t to_append = 0;
  for (y_absl::string_view piece : pieces) {
    ASSERT_NO_OVERLAP(*dest, piece);
    to_append += piece.size();
  }
  strings_internal::STLStringAppendUninitializedAmortized(dest, to_append);

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

void StrAppend(y_absl::Nonnull<TString*> dest, const AlphaNum& a) {
  ASSERT_NO_OVERLAP(*dest, a);
  TString::size_type old_size = dest->size();
  strings_internal::STLStringAppendUninitializedAmortized(dest, a.size());
  char* const begin = &(*dest)[0];
  char* out = begin + old_size;
  out = Append(out, a);
  assert(out == begin + dest->size());
}

void StrAppend(y_absl::Nonnull<TString*> dest, const AlphaNum& a,
               const AlphaNum& b) {
  ASSERT_NO_OVERLAP(*dest, a);
  ASSERT_NO_OVERLAP(*dest, b);
  TString::size_type old_size = dest->size();
  strings_internal::STLStringAppendUninitializedAmortized(dest,
                                                          a.size() + b.size());
  char* const begin = &(*dest)[0];
  char* out = begin + old_size;
  out = Append(out, a);
  out = Append(out, b);
  assert(out == begin + dest->size());
}

void StrAppend(y_absl::Nonnull<TString*> dest, const AlphaNum& a,
               const AlphaNum& b, const AlphaNum& c) {
  ASSERT_NO_OVERLAP(*dest, a);
  ASSERT_NO_OVERLAP(*dest, b);
  ASSERT_NO_OVERLAP(*dest, c);
  TString::size_type old_size = dest->size();
  strings_internal::STLStringAppendUninitializedAmortized(
      dest, a.size() + b.size() + c.size());
  char* const begin = &(*dest)[0];
  char* out = begin + old_size;
  out = Append(out, a);
  out = Append(out, b);
  out = Append(out, c);
  assert(out == begin + dest->size());
}

void StrAppend(y_absl::Nonnull<TString*> dest, const AlphaNum& a,
               const AlphaNum& b, const AlphaNum& c, const AlphaNum& d) {
  ASSERT_NO_OVERLAP(*dest, a);
  ASSERT_NO_OVERLAP(*dest, b);
  ASSERT_NO_OVERLAP(*dest, c);
  ASSERT_NO_OVERLAP(*dest, d);
  TString::size_type old_size = dest->size();
  strings_internal::STLStringAppendUninitializedAmortized(
      dest, a.size() + b.size() + c.size() + d.size());
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
