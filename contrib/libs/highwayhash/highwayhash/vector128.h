// Copyright 2016 Google Inc. All Rights Reserved.
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

#ifndef HIGHWAYHASH_VECTOR128_H_
#define HIGHWAYHASH_VECTOR128_H_

// Defines SIMD vector classes ("V2x64U") with overloaded arithmetic operators:
// const V2x64U masked_sum = (a + b) & m;
// This is shorter and more readable than compiler intrinsics:
// const __m128i masked_sum = _mm_and_si128(_mm_add_epi64(a, b), m);
// There is typically no runtime cost for these abstractions.
//
// The naming convention is VNxBBT where N is the number of lanes, BB the
// number of bits per lane and T is the lane type: unsigned integer (U),
// signed integer (I), or floating-point (F).

// WARNING: this is a "restricted" header because it is included from
// translation units compiled with different flags. This header and its
// dependencies must not define any function unless it is static inline and/or
// within namespace HH_TARGET_NAME. See arch_specific.h for details.

#include <stddef.h>
#include <stdint.h>

#include "highwayhash/arch_specific.h"
#include "highwayhash/compiler_specific.h"

// For auto-dependency generation, we need to include all headers but not their
// contents (otherwise compilation fails because -msse4.1 is not specified).
#ifndef HH_DISABLE_TARGET_SPECIFIC

// WARNING: smmintrin.h will also be included through immintrin.h in the AVX2
// translation unit, which is compiled with different flags. This risks ODR
// violations, and can cause crashes when functions are not inlined and the
// linker selects the AVX2 version. Unfortunately this include cannot reside
// within a namespace due to conflicts with other system headers. We need to
// assume all the intrinsic functions (defined as static inline by Clang's
// library and as extern inline by GCC) are in fact inlined. targets.bzl
// generates a test that verifies this by detecting duplicate symbols.
#include <smmintrin.h>  // SSE4.1

namespace highwayhash {
// To prevent ODR violations when including this from multiple translation
// units (TU) that are compiled with different flags, the contents must reside
// in a namespace whose name is unique to the TU. NOTE: this behavior is
// incompatible with precompiled modules and requires textual inclusion instead.
namespace HH_TARGET_NAME {

// Primary template for 128-bit SSE4.1 vectors; only specializations are used.
template <typename T>
class V128 {};

template <>
class V128<uint8_t> {
 public:
  using Intrinsic = __m128i;
  using T = uint8_t;
  static constexpr size_t N = 16;

  // Leaves v_ uninitialized - typically used for output parameters.
  HH_INLINE V128() {}

  // Broadcasts i to all lanes (usually by loading from memory).
  HH_INLINE explicit V128(T i) : v_(_mm_set1_epi8(i)) {}

  // Copy from other vector.
  HH_INLINE explicit V128(const V128& other) : v_(other.v_) {}
  template <typename U>
  HH_INLINE explicit V128(const V128<U>& other) : v_(other) {}
  HH_INLINE V128& operator=(const V128& other) {
    v_ = other.v_;
    return *this;
  }

  // Convert from/to intrinsics.
  HH_INLINE V128(const Intrinsic& v) : v_(v) {}
  HH_INLINE V128& operator=(const Intrinsic& v) {
    v_ = v;
    return *this;
  }
  HH_INLINE operator Intrinsic() const { return v_; }

  // There are no greater-than comparison instructions for unsigned T.
  HH_INLINE V128 operator==(const V128& other) const {
    return V128(_mm_cmpeq_epi8(v_, other.v_));
  }

  HH_INLINE V128& operator+=(const V128& other) {
    v_ = _mm_add_epi8(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator-=(const V128& other) {
    v_ = _mm_sub_epi8(v_, other.v_);
    return *this;
  }

  HH_INLINE V128& operator&=(const V128& other) {
    v_ = _mm_and_si128(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator|=(const V128& other) {
    v_ = _mm_or_si128(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator^=(const V128& other) {
    v_ = _mm_xor_si128(v_, other.v_);
    return *this;
  }

 private:
  Intrinsic v_;
};

template <>
class V128<uint16_t> {
 public:
  using Intrinsic = __m128i;
  using T = uint16_t;
  static constexpr size_t N = 8;

  // Leaves v_ uninitialized - typically used for output parameters.
  HH_INLINE V128() {}

  // Lane 0 (p_0) is the lowest.
  HH_INLINE V128(T p_7, T p_6, T p_5, T p_4, T p_3, T p_2, T p_1, T p_0)
      : v_(_mm_set_epi16(p_7, p_6, p_5, p_4, p_3, p_2, p_1, p_0)) {}

  // Broadcasts i to all lanes (usually by loading from memory).
  HH_INLINE explicit V128(T i) : v_(_mm_set1_epi16(i)) {}

  // Copy from other vector.
  HH_INLINE explicit V128(const V128& other) : v_(other.v_) {}
  template <typename U>
  HH_INLINE explicit V128(const V128<U>& other) : v_(other) {}
  HH_INLINE V128& operator=(const V128& other) {
    v_ = other.v_;
    return *this;
  }

  // Convert from/to intrinsics.
  HH_INLINE V128(const Intrinsic& v) : v_(v) {}
  HH_INLINE V128& operator=(const Intrinsic& v) {
    v_ = v;
    return *this;
  }
  HH_INLINE operator Intrinsic() const { return v_; }

  // There are no greater-than comparison instructions for unsigned T.
  HH_INLINE V128 operator==(const V128& other) const {
    return V128(_mm_cmpeq_epi16(v_, other.v_));
  }

  HH_INLINE V128& operator+=(const V128& other) {
    v_ = _mm_add_epi16(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator-=(const V128& other) {
    v_ = _mm_sub_epi16(v_, other.v_);
    return *this;
  }

  HH_INLINE V128& operator&=(const V128& other) {
    v_ = _mm_and_si128(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator|=(const V128& other) {
    v_ = _mm_or_si128(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator^=(const V128& other) {
    v_ = _mm_xor_si128(v_, other.v_);
    return *this;
  }

  HH_INLINE V128& operator<<=(const int count) {
    v_ = _mm_slli_epi16(v_, count);
    return *this;
  }
  HH_INLINE V128& operator<<=(const Intrinsic& count) {
    v_ = _mm_sll_epi16(v_, count);
    return *this;
  }

  HH_INLINE V128& operator>>=(const int count) {
    v_ = _mm_srli_epi16(v_, count);
    return *this;
  }
  HH_INLINE V128& operator>>=(const Intrinsic& count) {
    v_ = _mm_srl_epi16(v_, count);
    return *this;
  }

 private:
  Intrinsic v_;
};

template <>
class V128<uint32_t> {
 public:
  using Intrinsic = __m128i;
  using T = uint32_t;
  static constexpr size_t N = 4;

  // Leaves v_ uninitialized - typically used for output parameters.
  HH_INLINE V128() {}

  // Lane 0 (p_0) is the lowest.
  HH_INLINE V128(T p_3, T p_2, T p_1, T p_0)
      : v_(_mm_set_epi32(p_3, p_2, p_1, p_0)) {}

  // Broadcasts i to all lanes (usually by loading from memory).
  HH_INLINE explicit V128(T i) : v_(_mm_set1_epi32(i)) {}

  // Copy from other vector.
  HH_INLINE explicit V128(const V128& other) : v_(other.v_) {}
  template <typename U>
  HH_INLINE explicit V128(const V128<U>& other) : v_(other) {}
  HH_INLINE V128& operator=(const V128& other) {
    v_ = other.v_;
    return *this;
  }

  // Convert from/to intrinsics.
  HH_INLINE V128(const Intrinsic& v) : v_(v) {}
  HH_INLINE V128& operator=(const Intrinsic& v) {
    v_ = v;
    return *this;
  }
  HH_INLINE operator Intrinsic() const { return v_; }

  // There are no greater-than comparison instructions for unsigned T.
  HH_INLINE V128 operator==(const V128& other) const {
    return V128(_mm_cmpeq_epi32(v_, other.v_));
  }

  HH_INLINE V128& operator+=(const V128& other) {
    v_ = _mm_add_epi32(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator-=(const V128& other) {
    v_ = _mm_sub_epi32(v_, other.v_);
    return *this;
  }

  HH_INLINE V128& operator&=(const V128& other) {
    v_ = _mm_and_si128(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator|=(const V128& other) {
    v_ = _mm_or_si128(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator^=(const V128& other) {
    v_ = _mm_xor_si128(v_, other.v_);
    return *this;
  }

  HH_INLINE V128& operator<<=(const int count) {
    v_ = _mm_slli_epi32(v_, count);
    return *this;
  }
  HH_INLINE V128& operator<<=(const Intrinsic& count) {
    v_ = _mm_sll_epi32(v_, count);
    return *this;
  }

  HH_INLINE V128& operator>>=(const int count) {
    v_ = _mm_srli_epi32(v_, count);
    return *this;
  }
  HH_INLINE V128& operator>>=(const Intrinsic& count) {
    v_ = _mm_srl_epi32(v_, count);
    return *this;
  }

 private:
  Intrinsic v_;
};

template <>
class V128<uint64_t> {
 public:
  using Intrinsic = __m128i;
  using T = uint64_t;
  static constexpr size_t N = 2;

  // Leaves v_ uninitialized - typically used for output parameters.
  HH_INLINE V128() {}

  // Lane 0 (p_0) is the lowest.
  HH_INLINE V128(T p_1, T p_0) : v_(_mm_set_epi64x(p_1, p_0)) {}

  // Broadcasts i to all lanes (usually by loading from memory).
  HH_INLINE explicit V128(T i) : v_(_mm_set_epi64x(i, i)) {}

  // Copy from other vector.
  HH_INLINE explicit V128(const V128& other) : v_(other.v_) {}
  template <typename U>
  HH_INLINE explicit V128(const V128<U>& other) : v_(other) {}
  HH_INLINE V128& operator=(const V128& other) {
    v_ = other.v_;
    return *this;
  }

  // Convert from/to intrinsics.
  HH_INLINE V128(const Intrinsic& v) : v_(v) {}
  HH_INLINE V128& operator=(const Intrinsic& v) {
    v_ = v;
    return *this;
  }
  HH_INLINE operator Intrinsic() const { return v_; }

  // There are no greater-than comparison instructions for unsigned T.
  HH_INLINE V128 operator==(const V128& other) const {
    return V128(_mm_cmpeq_epi64(v_, other.v_));
  }

  HH_INLINE V128& operator+=(const V128& other) {
    v_ = _mm_add_epi64(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator-=(const V128& other) {
    v_ = _mm_sub_epi64(v_, other.v_);
    return *this;
  }

  HH_INLINE V128& operator&=(const V128& other) {
    v_ = _mm_and_si128(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator|=(const V128& other) {
    v_ = _mm_or_si128(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator^=(const V128& other) {
    v_ = _mm_xor_si128(v_, other.v_);
    return *this;
  }

  HH_INLINE V128& operator<<=(const int count) {
    v_ = _mm_slli_epi64(v_, count);
    return *this;
  }
  HH_INLINE V128& operator<<=(const Intrinsic& count) {
    v_ = _mm_sll_epi64(v_, count);
    return *this;
  }

  HH_INLINE V128& operator>>=(const int count) {
    v_ = _mm_srli_epi64(v_, count);
    return *this;
  }
  HH_INLINE V128& operator>>=(const Intrinsic& count) {
    v_ = _mm_srl_epi64(v_, count);
    return *this;
  }

 private:
  Intrinsic v_;
};

template <>
class V128<float> {
 public:
  using Intrinsic = __m128;
  using T = float;
  static constexpr size_t N = 4;

  // Leaves v_ uninitialized - typically used for output parameters.
  HH_INLINE V128() {}

  // Lane 0 (p_0) is the lowest.
  HH_INLINE V128(T p_3, T p_2, T p_1, T p_0)
      : v_(_mm_set_ps(p_3, p_2, p_1, p_0)) {}

  // Broadcasts to all lanes.
  HH_INLINE explicit V128(T f) : v_(_mm_set1_ps(f)) {}

  // Copy from other vector.
  HH_INLINE explicit V128(const V128& other) : v_(other.v_) {}
  template <typename U>
  HH_INLINE explicit V128(const V128<U>& other) : v_(other) {}
  HH_INLINE V128& operator=(const V128& other) {
    v_ = other.v_;
    return *this;
  }

  // Convert from/to intrinsics.
  HH_INLINE V128(const Intrinsic& v) : v_(v) {}
  HH_INLINE V128& operator=(const Intrinsic& v) {
    v_ = v;
    return *this;
  }
  HH_INLINE operator Intrinsic() const { return v_; }

  HH_INLINE V128 operator==(const V128& other) const {
    return V128(_mm_cmpeq_ps(v_, other.v_));
  }
  HH_INLINE V128 operator<(const V128& other) const {
    return V128(_mm_cmplt_ps(v_, other.v_));
  }
  HH_INLINE V128 operator>(const V128& other) const {
    return V128(_mm_cmplt_ps(other.v_, v_));
  }

  HH_INLINE V128& operator*=(const V128& other) {
    v_ = _mm_mul_ps(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator/=(const V128& other) {
    v_ = _mm_div_ps(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator+=(const V128& other) {
    v_ = _mm_add_ps(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator-=(const V128& other) {
    v_ = _mm_sub_ps(v_, other.v_);
    return *this;
  }

  HH_INLINE V128& operator&=(const V128& other) {
    v_ = _mm_and_ps(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator|=(const V128& other) {
    v_ = _mm_or_ps(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator^=(const V128& other) {
    v_ = _mm_xor_ps(v_, other.v_);
    return *this;
  }

 private:
  Intrinsic v_;
};

template <>
class V128<double> {
 public:
  using Intrinsic = __m128d;
  using T = double;
  static constexpr size_t N = 2;

  // Leaves v_ uninitialized - typically used for output parameters.
  HH_INLINE V128() {}

  // Lane 0 (p_0) is the lowest.
  HH_INLINE V128(T p_1, T p_0) : v_(_mm_set_pd(p_1, p_0)) {}

  // Broadcasts to all lanes.
  HH_INLINE explicit V128(T f) : v_(_mm_set1_pd(f)) {}

  // Copy from other vector.
  HH_INLINE explicit V128(const V128& other) : v_(other.v_) {}
  template <typename U>
  HH_INLINE explicit V128(const V128<U>& other) : v_(other) {}
  HH_INLINE V128& operator=(const V128& other) {
    v_ = other.v_;
    return *this;
  }

  // Convert from/to intrinsics.
  HH_INLINE V128(const Intrinsic& v) : v_(v) {}
  HH_INLINE V128& operator=(const Intrinsic& v) {
    v_ = v;
    return *this;
  }
  HH_INLINE operator Intrinsic() const { return v_; }

  HH_INLINE V128 operator==(const V128& other) const {
    return V128(_mm_cmpeq_pd(v_, other.v_));
  }
  HH_INLINE V128 operator<(const V128& other) const {
    return V128(_mm_cmplt_pd(v_, other.v_));
  }
  HH_INLINE V128 operator>(const V128& other) const {
    return V128(_mm_cmplt_pd(other.v_, v_));
  }

  HH_INLINE V128& operator*=(const V128& other) {
    v_ = _mm_mul_pd(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator/=(const V128& other) {
    v_ = _mm_div_pd(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator+=(const V128& other) {
    v_ = _mm_add_pd(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator-=(const V128& other) {
    v_ = _mm_sub_pd(v_, other.v_);
    return *this;
  }

  HH_INLINE V128& operator&=(const V128& other) {
    v_ = _mm_and_pd(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator|=(const V128& other) {
    v_ = _mm_or_pd(v_, other.v_);
    return *this;
  }
  HH_INLINE V128& operator^=(const V128& other) {
    v_ = _mm_xor_pd(v_, other.v_);
    return *this;
  }

 private:
  Intrinsic v_;
};

// Nonmember functions for any V128 via member functions.

template <typename T>
HH_INLINE V128<T> operator*(const V128<T>& left, const V128<T>& right) {
  V128<T> t(left);
  return t *= right;
}

template <typename T>
HH_INLINE V128<T> operator/(const V128<T>& left, const V128<T>& right) {
  V128<T> t(left);
  return t /= right;
}

template <typename T>
HH_INLINE V128<T> operator+(const V128<T>& left, const V128<T>& right) {
  V128<T> t(left);
  return t += right;
}

template <typename T>
HH_INLINE V128<T> operator-(const V128<T>& left, const V128<T>& right) {
  V128<T> t(left);
  return t -= right;
}

template <typename T>
HH_INLINE V128<T> operator&(const V128<T>& left, const V128<T>& right) {
  V128<T> t(left);
  return t &= right;
}

template <typename T>
HH_INLINE V128<T> operator|(const V128<T>& left, const V128<T>& right) {
  V128<T> t(left);
  return t |= right;
}

template <typename T>
HH_INLINE V128<T> operator^(const V128<T>& left, const V128<T>& right) {
  V128<T> t(left);
  return t ^= right;
}

template <typename T>
HH_INLINE V128<T> operator<<(const V128<T>& v, const int count) {
  V128<T> t(v);
  return t <<= count;
}

template <typename T>
HH_INLINE V128<T> operator>>(const V128<T>& v, const int count) {
  V128<T> t(v);
  return t >>= count;
}

template <typename T>
HH_INLINE V128<T> operator<<(const V128<T>& v, const __m128i& count) {
  V128<T> t(v);
  return t <<= count;
}

template <typename T>
HH_INLINE V128<T> operator>>(const V128<T>& v, const __m128i& count) {
  V128<T> t(v);
  return t >>= count;
}

using V16x8U = V128<uint8_t>;
using V8x16U = V128<uint16_t>;
using V4x32U = V128<uint32_t>;
using V2x64U = V128<uint64_t>;
using V4x32F = V128<float>;
using V2x64F = V128<double>;

// Load/Store for any V128.

// We differentiate between targets' vector types via template specialization.
// Calling Load<V>(floats) is more natural than Load(V8x32F(), floats) and may
// generate better code in unoptimized builds. Only declare the primary
// templates to avoid needing mutual exclusion with vector256.

template <class V>
HH_INLINE V Load(const typename V::T* const HH_RESTRICT from);

template <class V>
HH_INLINE V LoadUnaligned(const typename V::T* const HH_RESTRICT from);

// "from" must be vector-aligned.
template <>
HH_INLINE V16x8U Load<V16x8U>(const V16x8U::T* const HH_RESTRICT from) {
  const __m128i* const HH_RESTRICT p = reinterpret_cast<const __m128i*>(from);
  return V16x8U(_mm_load_si128(p));
}
template <>
HH_INLINE V8x16U Load<V8x16U>(const V8x16U::T* const HH_RESTRICT from) {
  const __m128i* const HH_RESTRICT p = reinterpret_cast<const __m128i*>(from);
  return V8x16U(_mm_load_si128(p));
}
template <>
HH_INLINE V4x32U Load<V4x32U>(const V4x32U::T* const HH_RESTRICT from) {
  const __m128i* const HH_RESTRICT p = reinterpret_cast<const __m128i*>(from);
  return V4x32U(_mm_load_si128(p));
}
template <>
HH_INLINE V2x64U Load<V2x64U>(const V2x64U::T* const HH_RESTRICT from) {
  const __m128i* const HH_RESTRICT p = reinterpret_cast<const __m128i*>(from);
  return V2x64U(_mm_load_si128(p));
}
template <>
HH_INLINE V4x32F Load<V4x32F>(const V4x32F::T* const HH_RESTRICT from) {
  return V4x32F(_mm_load_ps(from));
}
template <>
HH_INLINE V2x64F Load<V2x64F>(const V2x64F::T* const HH_RESTRICT from) {
  return V2x64F(_mm_load_pd(from));
}

template <>
HH_INLINE V16x8U
LoadUnaligned<V16x8U>(const V16x8U::T* const HH_RESTRICT from) {
  const __m128i* const HH_RESTRICT p = reinterpret_cast<const __m128i*>(from);
  return V16x8U(_mm_loadu_si128(p));
}
template <>
HH_INLINE V8x16U
LoadUnaligned<V8x16U>(const V8x16U::T* const HH_RESTRICT from) {
  const __m128i* const HH_RESTRICT p = reinterpret_cast<const __m128i*>(from);
  return V8x16U(_mm_loadu_si128(p));
}
template <>
HH_INLINE V4x32U
LoadUnaligned<V4x32U>(const V4x32U::T* const HH_RESTRICT from) {
  const __m128i* const HH_RESTRICT p = reinterpret_cast<const __m128i*>(from);
  return V4x32U(_mm_loadu_si128(p));
}
template <>
HH_INLINE V2x64U
LoadUnaligned<V2x64U>(const V2x64U::T* const HH_RESTRICT from) {
  const __m128i* const HH_RESTRICT p = reinterpret_cast<const __m128i*>(from);
  return V2x64U(_mm_loadu_si128(p));
}
template <>
HH_INLINE V4x32F
LoadUnaligned<V4x32F>(const V4x32F::T* const HH_RESTRICT from) {
  return V4x32F(_mm_loadu_ps(from));
}
template <>
HH_INLINE V2x64F
LoadUnaligned<V2x64F>(const V2x64F::T* const HH_RESTRICT from) {
  return V2x64F(_mm_loadu_pd(from));
}

// "to" must be vector-aligned.
template <typename T>
HH_INLINE void Store(const V128<T>& v, T* const HH_RESTRICT to) {
  _mm_store_si128(reinterpret_cast<__m128i * HH_RESTRICT>(to), v);
}
HH_INLINE void Store(const V128<float>& v, float* const HH_RESTRICT to) {
  _mm_store_ps(to, v);
}
HH_INLINE void Store(const V128<double>& v, double* const HH_RESTRICT to) {
  _mm_store_pd(to, v);
}

template <typename T>
HH_INLINE void StoreUnaligned(const V128<T>& v, T* const HH_RESTRICT to) {
  _mm_storeu_si128(reinterpret_cast<__m128i * HH_RESTRICT>(to), v);
}
HH_INLINE void StoreUnaligned(const V128<float>& v,
                              float* const HH_RESTRICT to) {
  _mm_storeu_ps(to, v);
}
HH_INLINE void StoreUnaligned(const V128<double>& v,
                              double* const HH_RESTRICT to) {
  _mm_storeu_pd(to, v);
}

// Writes directly to (aligned) memory, bypassing the cache. This is useful for
// data that will not be read again in the near future.
template <typename T>
HH_INLINE void Stream(const V128<T>& v, T* const HH_RESTRICT to) {
  _mm_stream_si128(reinterpret_cast<__m128i * HH_RESTRICT>(to), v);
}
HH_INLINE void Stream(const V128<float>& v, float* const HH_RESTRICT to) {
  _mm_stream_ps(to, v);
}
HH_INLINE void Stream(const V128<double>& v, double* const HH_RESTRICT to) {
  _mm_stream_pd(to, v);
}

// Miscellaneous functions.

template <typename T>
HH_INLINE V128<T> RotateLeft(const V128<T>& v, const int count) {
  constexpr size_t num_bits = sizeof(T) * 8;
  return (v << count) | (v >> (num_bits - count));
}

template <typename T>
HH_INLINE V128<T> AndNot(const V128<T>& neg_mask, const V128<T>& values) {
  return V128<T>(_mm_andnot_si128(neg_mask, values));
}
template <>
HH_INLINE V128<float> AndNot(const V128<float>& neg_mask,
                             const V128<float>& values) {
  return V128<float>(_mm_andnot_ps(neg_mask, values));
}
template <>
HH_INLINE V128<double> AndNot(const V128<double>& neg_mask,
                              const V128<double>& values) {
  return V128<double>(_mm_andnot_pd(neg_mask, values));
}

HH_INLINE V4x32F Select(const V4x32F& a, const V4x32F& b, const V4x32F& mask) {
  return V4x32F(_mm_blendv_ps(a, b, mask));
}

HH_INLINE V2x64F Select(const V2x64F& a, const V2x64F& b, const V2x64F& mask) {
  return V2x64F(_mm_blendv_pd(a, b, mask));
}

// Min/Max

HH_INLINE V16x8U Min(const V16x8U& v0, const V16x8U& v1) {
  return V16x8U(_mm_min_epu8(v0, v1));
}

HH_INLINE V16x8U Max(const V16x8U& v0, const V16x8U& v1) {
  return V16x8U(_mm_max_epu8(v0, v1));
}

HH_INLINE V8x16U Min(const V8x16U& v0, const V8x16U& v1) {
  return V8x16U(_mm_min_epu16(v0, v1));
}

HH_INLINE V8x16U Max(const V8x16U& v0, const V8x16U& v1) {
  return V8x16U(_mm_max_epu16(v0, v1));
}

HH_INLINE V4x32U Min(const V4x32U& v0, const V4x32U& v1) {
  return V4x32U(_mm_min_epu32(v0, v1));
}

HH_INLINE V4x32U Max(const V4x32U& v0, const V4x32U& v1) {
  return V4x32U(_mm_max_epu32(v0, v1));
}

HH_INLINE V4x32F Min(const V4x32F& v0, const V4x32F& v1) {
  return V4x32F(_mm_min_ps(v0, v1));
}

HH_INLINE V4x32F Max(const V4x32F& v0, const V4x32F& v1) {
  return V4x32F(_mm_max_ps(v0, v1));
}

HH_INLINE V2x64F Min(const V2x64F& v0, const V2x64F& v1) {
  return V2x64F(_mm_min_pd(v0, v1));
}

HH_INLINE V2x64F Max(const V2x64F& v0, const V2x64F& v1) {
  return V2x64F(_mm_max_pd(v0, v1));
}

}  // namespace HH_TARGET_NAME
}  // namespace highwayhash

#endif  // HH_DISABLE_TARGET_SPECIFIC
#endif  // HIGHWAYHASH_VECTOR128_H_
