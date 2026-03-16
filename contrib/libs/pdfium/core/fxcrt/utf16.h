// Copyright 2023 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_UTF16_H_
#define CORE_FXCRT_UTF16_H_

#include "core/fxcrt/check.h"

namespace pdfium {

// The number of suffix bits in a UTF-16 surrogate.
inline constexpr int kSurrogateBits = 10;

// A bitmask for the suffix of a UTF-16 surrogate.
inline constexpr char16_t kSurrogateMask = (1 << kSurrogateBits) - 1;

// The first supplementary code point, `U+10000`.
inline constexpr char32_t kMinimumSupplementaryCodePoint = 0x10000;

// The last supplementary code point, `U+10FFFF`.
inline constexpr char32_t kMaximumSupplementaryCodePoint =
    kMinimumSupplementaryCodePoint +
    (kSurrogateMask << kSurrogateBits | kSurrogateMask);

// The first UTF-16 high surrogate code unit, `U+D800`.
inline constexpr char16_t kMinimumHighSurrogateCodeUnit = 0xd800;

// The last UTF-16 high surrogate code unit, `U+DBFF`.
inline constexpr char16_t kMaximumHighSurrogateCodeUnit =
    kMinimumHighSurrogateCodeUnit | kSurrogateMask;

// The first UTF-16 low surrogate code unit, `U+DC00`.
inline constexpr char16_t kMinimumLowSurrogateCodeUnit =
    kMaximumHighSurrogateCodeUnit + 1;

// The last UTF-16 low surrogate code unit, `U+DFFF`.
inline constexpr char16_t kMaximumLowSurrogateCodeUnit =
    kMinimumLowSurrogateCodeUnit | kSurrogateMask;

// Returns `true` if `code_point` is in a supplementary plane, and therefore
// requires encoding as a UTF-16 surrogate pair.
constexpr bool IsSupplementary(char32_t code_point) {
  return code_point >= kMinimumSupplementaryCodePoint &&
         code_point <= kMaximumSupplementaryCodePoint;
}

// Returns `true` if `code_point` is a UTF-16 high surrogate.
constexpr bool IsHighSurrogate(char32_t code_point) {
  return code_point >= kMinimumHighSurrogateCodeUnit &&
         code_point <= kMaximumHighSurrogateCodeUnit;
}

// Returns `true` if `code_point` is a UTF-16 low surrogate.
constexpr bool IsLowSurrogate(char32_t code_point) {
  return code_point >= kMinimumLowSurrogateCodeUnit &&
         code_point <= kMaximumLowSurrogateCodeUnit;
}

// A UTF-16 surrogate pair.
class SurrogatePair final {
 public:
  // Constructs a surrogate pair from a high and a low surrogate.
  constexpr SurrogatePair(char16_t high, char16_t low)
      : high_(high), low_(low) {
    DCHECK(IsHighSurrogate(high_));
    DCHECK(IsLowSurrogate(low_));
  }

  // Constructs a surrogate pair from a code point.
  explicit constexpr SurrogatePair(char32_t code_point)
      : high_(GetHighSurrogate(code_point)), low_(GetLowSurrogate(code_point)) {
    // This constructor initializes `high_` and `low_` using helper functions
    // because C++17 requires it for `constexpr` constructors.
    DCHECK(IsSupplementary(code_point));
  }

  constexpr char16_t high() const { return high_; }
  constexpr char16_t low() const { return low_; }

  // Decodes this surrogate pair to a code point.
  constexpr char32_t ToCodePoint() const {
    char32_t code_point = low_ & kSurrogateMask;
    code_point |= (high_ & kSurrogateMask) << kSurrogateBits;
    return kMinimumSupplementaryCodePoint + code_point;
  }

 private:
  static constexpr char16_t GetHighSurrogate(char32_t code_point) {
    code_point -= kMinimumSupplementaryCodePoint;
    char16_t code_unit = (code_point >> kSurrogateBits) & kSurrogateMask;
    return kMinimumHighSurrogateCodeUnit | code_unit;
  }

  static constexpr char16_t GetLowSurrogate(char32_t code_point) {
    code_point -= kMinimumSupplementaryCodePoint;
    char16_t code_unit = code_point & kSurrogateMask;
    return kMinimumLowSurrogateCodeUnit | code_unit;
  }

  char16_t high_;
  char16_t low_;
};

}  // namespace pdfium

#endif  // CORE_FXCRT_UTF16_H_
