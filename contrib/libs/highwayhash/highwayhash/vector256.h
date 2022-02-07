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

#ifndef HIGHWAYHASH_VECTOR256_H_
#define HIGHWAYHASH_VECTOR256_H_

// Defines SIMD vector classes ("V4x64U") with overloaded arithmetic operators:
// const V4x64U masked_sum = (a + b) & m;
// This is shorter and more readable than compiler intrinsics:
// const __m256i masked_sum = _mm256_and_si256(_mm256_add_epi64(a, b), m);
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
// contents (otherwise compilation fails because -mavx2 is not specified).
#ifndef HH_DISABLE_TARGET_SPECIFIC

// (This include cannot be moved within a namespace due to conflicts with
// other system headers; see the comment in hh_sse41.h.)
#include <immintrin.h>

namespace highwayhash {
// To prevent ODR violations when including this from multiple translation
// units (TU) that are compiled with different flags, the contents must reside
// in a namespace whose name is unique to the TU. NOTE: this behavior is
// incompatible with precompiled modules and requires textual inclusion instead.
namespace HH_TARGET_NAME {

// Primary template for 256-bit AVX2 vectors; only specializations are used.
template <typename T>
class V256 {};

template <>
class V256<uint8_t> {
 public:
  using Intrinsic = __m256i;
  using T = uint8_t;
  static constexpr size_t N = 32;

  // Leaves v_ uninitialized - typically used for output parameters.
  HH_INLINE V256() {}

  // Broadcasts i to all lanes.
  HH_INLINE explicit V256(T i)
      : v_(_mm256_broadcastb_epi8(_mm_cvtsi32_si128(i))) {}

  // Copy from other vector.
  HH_INLINE explicit V256(const V256& other) : v_(other.v_) {}
  template <typename U>
  HH_INLINE explicit V256(const V256<U>& other) : v_(other) {}
  HH_INLINE V256& operator=(const V256& other) {
    v_ = other.v_;
    return *this;
  }

  // Convert from/to intrinsics.
  HH_INLINE V256(const Intrinsic& v) : v_(v) {}
  HH_INLINE V256& operator=(const Intrinsic& v) {
    v_ = v;
    return *this;
  }
  HH_INLINE operator Intrinsic() const { return v_; }

  // There are no greater-than comparison instructions for unsigned T.
  HH_INLINE V256 operator==(const V256& other) const {
    return V256(_mm256_cmpeq_epi8(v_, other.v_));
  }

  HH_INLINE V256& operator+=(const V256& other) {
    v_ = _mm256_add_epi8(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator-=(const V256& other) {
    v_ = _mm256_sub_epi8(v_, other.v_);
    return *this;
  }

  HH_INLINE V256& operator&=(const V256& other) {
    v_ = _mm256_and_si256(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator|=(const V256& other) {
    v_ = _mm256_or_si256(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator^=(const V256& other) {
    v_ = _mm256_xor_si256(v_, other.v_);
    return *this;
  }

 private:
  Intrinsic v_;
};

template <>
class V256<uint16_t> {
 public:
  using Intrinsic = __m256i;
  using T = uint16_t;
  static constexpr size_t N = 16;

  // Leaves v_ uninitialized - typically used for output parameters.
  HH_INLINE V256() {}

  // Lane 0 (p_0) is the lowest.
  HH_INLINE V256(T p_F, T p_E, T p_D, T p_C, T p_B, T p_A, T p_9, T p_8, T p_7,
                 T p_6, T p_5, T p_4, T p_3, T p_2, T p_1, T p_0)
      : v_(_mm256_set_epi16(p_F, p_E, p_D, p_C, p_B, p_A, p_9, p_8, p_7, p_6,
                            p_5, p_4, p_3, p_2, p_1, p_0)) {}

  // Broadcasts i to all lanes.
  HH_INLINE explicit V256(T i)
      : v_(_mm256_broadcastw_epi16(_mm_cvtsi32_si128(i))) {}

  // Copy from other vector.
  HH_INLINE explicit V256(const V256& other) : v_(other.v_) {}
  template <typename U>
  HH_INLINE explicit V256(const V256<U>& other) : v_(other) {}
  HH_INLINE V256& operator=(const V256& other) {
    v_ = other.v_;
    return *this;
  }

  // Convert from/to intrinsics.
  HH_INLINE V256(const Intrinsic& v) : v_(v) {}
  HH_INLINE V256& operator=(const Intrinsic& v) {
    v_ = v;
    return *this;
  }
  HH_INLINE operator Intrinsic() const { return v_; }

  // There are no greater-than comparison instructions for unsigned T.
  HH_INLINE V256 operator==(const V256& other) const {
    return V256(_mm256_cmpeq_epi16(v_, other.v_));
  }

  HH_INLINE V256& operator+=(const V256& other) {
    v_ = _mm256_add_epi16(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator-=(const V256& other) {
    v_ = _mm256_sub_epi16(v_, other.v_);
    return *this;
  }

  HH_INLINE V256& operator&=(const V256& other) {
    v_ = _mm256_and_si256(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator|=(const V256& other) {
    v_ = _mm256_or_si256(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator^=(const V256& other) {
    v_ = _mm256_xor_si256(v_, other.v_);
    return *this;
  }

  HH_INLINE V256& operator<<=(const int count) {
    v_ = _mm256_slli_epi16(v_, count);
    return *this;
  }

  HH_INLINE V256& operator>>=(const int count) {
    v_ = _mm256_srli_epi16(v_, count);
    return *this;
  }

 private:
  Intrinsic v_;
};

template <>
class V256<uint32_t> {
 public:
  using Intrinsic = __m256i;
  using T = uint32_t;
  static constexpr size_t N = 8;

  // Leaves v_ uninitialized - typically used for output parameters.
  HH_INLINE V256() {}

  // Lane 0 (p_0) is the lowest.
  HH_INLINE V256(T p_7, T p_6, T p_5, T p_4, T p_3, T p_2, T p_1, T p_0)
      : v_(_mm256_set_epi32(p_7, p_6, p_5, p_4, p_3, p_2, p_1, p_0)) {}

  // Broadcasts i to all lanes.
  HH_INLINE explicit V256(T i)
      : v_(_mm256_broadcastd_epi32(_mm_cvtsi32_si128(i))) {}

  // Copy from other vector.
  HH_INLINE explicit V256(const V256& other) : v_(other.v_) {}
  template <typename U>
  HH_INLINE explicit V256(const V256<U>& other) : v_(other) {}
  HH_INLINE V256& operator=(const V256& other) {
    v_ = other.v_;
    return *this;
  }

  // Convert from/to intrinsics.
  HH_INLINE V256(const Intrinsic& v) : v_(v) {}
  HH_INLINE V256& operator=(const Intrinsic& v) {
    v_ = v;
    return *this;
  }
  HH_INLINE operator Intrinsic() const { return v_; }

  // There are no greater-than comparison instructions for unsigned T.
  HH_INLINE V256 operator==(const V256& other) const {
    return V256(_mm256_cmpeq_epi32(v_, other.v_));
  }

  HH_INLINE V256& operator+=(const V256& other) {
    v_ = _mm256_add_epi32(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator-=(const V256& other) {
    v_ = _mm256_sub_epi32(v_, other.v_);
    return *this;
  }

  HH_INLINE V256& operator&=(const V256& other) {
    v_ = _mm256_and_si256(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator|=(const V256& other) {
    v_ = _mm256_or_si256(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator^=(const V256& other) {
    v_ = _mm256_xor_si256(v_, other.v_);
    return *this;
  }

  HH_INLINE V256& operator<<=(const int count) {
    v_ = _mm256_slli_epi32(v_, count);
    return *this;
  }

  HH_INLINE V256& operator>>=(const int count) {
    v_ = _mm256_srli_epi32(v_, count);
    return *this;
  }

 private:
  Intrinsic v_;
};

template <>
class V256<uint64_t> {
 public:
  using Intrinsic = __m256i;
  using T = uint64_t;
  static constexpr size_t N = 4;

  // Leaves v_ uninitialized - typically used for output parameters.
  HH_INLINE V256() {}

  // Lane 0 (p_0) is the lowest.
  HH_INLINE V256(T p_3, T p_2, T p_1, T p_0)
      : v_(_mm256_set_epi64x(p_3, p_2, p_1, p_0)) {}

  // Broadcasts i to all lanes.
  HH_INLINE explicit V256(T i)
      : v_(_mm256_broadcastq_epi64(_mm_cvtsi64_si128(i))) {}

  // Copy from other vector.
  HH_INLINE explicit V256(const V256& other) : v_(other.v_) {}
  template <typename U>
  HH_INLINE explicit V256(const V256<U>& other) : v_(other) {}
  HH_INLINE V256& operator=(const V256& other) {
    v_ = other.v_;
    return *this;
  }

  // Convert from/to intrinsics.
  HH_INLINE V256(const Intrinsic& v) : v_(v) {}
  HH_INLINE V256& operator=(const Intrinsic& v) {
    v_ = v;
    return *this;
  }
  HH_INLINE operator Intrinsic() const { return v_; }

  // There are no greater-than comparison instructions for unsigned T.
  HH_INLINE V256 operator==(const V256& other) const {
    return V256(_mm256_cmpeq_epi64(v_, other.v_));
  }

  HH_INLINE V256& operator+=(const V256& other) {
    v_ = _mm256_add_epi64(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator-=(const V256& other) {
    v_ = _mm256_sub_epi64(v_, other.v_);
    return *this;
  }

  HH_INLINE V256& operator&=(const V256& other) {
    v_ = _mm256_and_si256(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator|=(const V256& other) {
    v_ = _mm256_or_si256(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator^=(const V256& other) {
    v_ = _mm256_xor_si256(v_, other.v_);
    return *this;
  }

  HH_INLINE V256& operator<<=(const int count) {
    v_ = _mm256_slli_epi64(v_, count);
    return *this;
  }

  HH_INLINE V256& operator>>=(const int count) {
    v_ = _mm256_srli_epi64(v_, count);
    return *this;
  }

 private:
  Intrinsic v_;
};

template <>
class V256<float> {
 public:
  using Intrinsic = __m256;
  using T = float;
  static constexpr size_t N = 8;

  // Leaves v_ uninitialized - typically used for output parameters.
  HH_INLINE V256() {}

  // Lane 0 (p_0) is the lowest.
  HH_INLINE V256(T p_7, T p_6, T p_5, T p_4, T p_3, T p_2, T p_1, T p_0)
      : v_(_mm256_set_ps(p_7, p_6, p_5, p_4, p_3, p_2, p_1, p_0)) {}

  // Broadcasts to all lanes.
  HH_INLINE explicit V256(T f) : v_(_mm256_set1_ps(f)) {}

  // Copy from other vector.
  HH_INLINE explicit V256(const V256& other) : v_(other.v_) {}
  template <typename U>
  HH_INLINE explicit V256(const V256<U>& other) : v_(other) {}
  HH_INLINE V256& operator=(const V256& other) {
    v_ = other.v_;
    return *this;
  }

  // Convert from/to intrinsics.
  HH_INLINE V256(const Intrinsic& v) : v_(v) {}
  HH_INLINE V256& operator=(const Intrinsic& v) {
    v_ = v;
    return *this;
  }
  HH_INLINE operator Intrinsic() const { return v_; }

  HH_INLINE V256 operator==(const V256& other) const {
    return V256(_mm256_cmp_ps(v_, other.v_, 0));
  }
  HH_INLINE V256 operator<(const V256& other) const {
    return V256(_mm256_cmp_ps(v_, other.v_, 1));
  }
  HH_INLINE V256 operator>(const V256& other) const {
    return V256(_mm256_cmp_ps(other.v_, v_, 1));
  }

  HH_INLINE V256& operator*=(const V256& other) {
    v_ = _mm256_mul_ps(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator/=(const V256& other) {
    v_ = _mm256_div_ps(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator+=(const V256& other) {
    v_ = _mm256_add_ps(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator-=(const V256& other) {
    v_ = _mm256_sub_ps(v_, other.v_);
    return *this;
  }

  HH_INLINE V256& operator&=(const V256& other) {
    v_ = _mm256_and_ps(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator|=(const V256& other) {
    v_ = _mm256_or_ps(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator^=(const V256& other) {
    v_ = _mm256_xor_ps(v_, other.v_);
    return *this;
  }

 private:
  Intrinsic v_;
};

template <>
class V256<double> {
 public:
  using Intrinsic = __m256d;
  using T = double;
  static constexpr size_t N = 4;

  // Leaves v_ uninitialized - typically used for output parameters.
  HH_INLINE V256() {}

  // Lane 0 (p_0) is the lowest.
  HH_INLINE V256(T p_3, T p_2, T p_1, T p_0)
      : v_(_mm256_set_pd(p_3, p_2, p_1, p_0)) {}

  // Broadcasts to all lanes.
  HH_INLINE explicit V256(T f) : v_(_mm256_set1_pd(f)) {}

  // Copy from other vector.
  HH_INLINE explicit V256(const V256& other) : v_(other.v_) {}
  template <typename U>
  HH_INLINE explicit V256(const V256<U>& other) : v_(other) {}
  HH_INLINE V256& operator=(const V256& other) {
    v_ = other.v_;
    return *this;
  }

  // Convert from/to intrinsics.
  HH_INLINE V256(const Intrinsic& v) : v_(v) {}
  HH_INLINE V256& operator=(const Intrinsic& v) {
    v_ = v;
    return *this;
  }
  HH_INLINE operator Intrinsic() const { return v_; }

  HH_INLINE V256 operator==(const V256& other) const {
    return V256(_mm256_cmp_pd(v_, other.v_, 0));
  }
  HH_INLINE V256 operator<(const V256& other) const {
    return V256(_mm256_cmp_pd(v_, other.v_, 1));
  }
  HH_INLINE V256 operator>(const V256& other) const {
    return V256(_mm256_cmp_pd(other.v_, v_, 1));
  }

  HH_INLINE V256& operator*=(const V256& other) {
    v_ = _mm256_mul_pd(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator/=(const V256& other) {
    v_ = _mm256_div_pd(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator+=(const V256& other) {
    v_ = _mm256_add_pd(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator-=(const V256& other) {
    v_ = _mm256_sub_pd(v_, other.v_);
    return *this;
  }

  HH_INLINE V256& operator&=(const V256& other) {
    v_ = _mm256_and_pd(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator|=(const V256& other) {
    v_ = _mm256_or_pd(v_, other.v_);
    return *this;
  }
  HH_INLINE V256& operator^=(const V256& other) {
    v_ = _mm256_xor_pd(v_, other.v_);
    return *this;
  }

 private:
  Intrinsic v_;
};

// Nonmember functions for any V256 via member functions.

template <typename T>
HH_INLINE V256<T> operator*(const V256<T>& left, const V256<T>& right) {
  V256<T> t(left);
  return t *= right;
}

template <typename T>
HH_INLINE V256<T> operator/(const V256<T>& left, const V256<T>& right) {
  V256<T> t(left);
  return t /= right;
}

template <typename T>
HH_INLINE V256<T> operator+(const V256<T>& left, const V256<T>& right) {
  V256<T> t(left);
  return t += right;
}

template <typename T>
HH_INLINE V256<T> operator-(const V256<T>& left, const V256<T>& right) {
  V256<T> t(left);
  return t -= right;
}

template <typename T>
HH_INLINE V256<T> operator&(const V256<T>& left, const V256<T>& right) {
  V256<T> t(left);
  return t &= right;
}

template <typename T>
HH_INLINE V256<T> operator|(const V256<T> left, const V256<T>& right) {
  V256<T> t(left);
  return t |= right;
}

template <typename T>
HH_INLINE V256<T> operator^(const V256<T>& left, const V256<T>& right) {
  V256<T> t(left);
  return t ^= right;
}

template <typename T>
HH_INLINE V256<T> operator<<(const V256<T>& v, const int count) {
  V256<T> t(v);
  return t <<= count;
}

template <typename T>
HH_INLINE V256<T> operator>>(const V256<T>& v, const int count) {
  V256<T> t(v);
  return t >>= count;
}

// We do not provide operator<<(V, __m128i) because it has 4 cycle latency
// (to broadcast the shift count). It is faster to use sllv_epi64 etc. instead.

using V32x8U = V256<uint8_t>;
using V16x16U = V256<uint16_t>;
using V8x32U = V256<uint32_t>;
using V4x64U = V256<uint64_t>;
using V8x32F = V256<float>;
using V4x64F = V256<double>;

// Load/Store for any V256.

// We differentiate between targets' vector types via template specialization.
// Calling Load<V>(floats) is more natural than Load(V8x32F(), floats) and may
// generate better code in unoptimized builds. Only declare the primary
// templates to avoid needing mutual exclusion with vector128.

template <class V>
HH_INLINE V Load(const typename V::T* const HH_RESTRICT from);

template <class V>
HH_INLINE V LoadUnaligned(const typename V::T* const HH_RESTRICT from);

template <>
HH_INLINE V32x8U Load(const V32x8U::T* const HH_RESTRICT from) {
  const __m256i* const HH_RESTRICT p = reinterpret_cast<const __m256i*>(from);
  return V32x8U(_mm256_load_si256(p));
}
template <>
HH_INLINE V16x16U Load(const V16x16U::T* const HH_RESTRICT from) {
  const __m256i* const HH_RESTRICT p = reinterpret_cast<const __m256i*>(from);
  return V16x16U(_mm256_load_si256(p));
}
template <>
HH_INLINE V8x32U Load(const V8x32U::T* const HH_RESTRICT from) {
  const __m256i* const HH_RESTRICT p = reinterpret_cast<const __m256i*>(from);
  return V8x32U(_mm256_load_si256(p));
}
template <>
HH_INLINE V4x64U Load(const V4x64U::T* const HH_RESTRICT from) {
  const __m256i* const HH_RESTRICT p = reinterpret_cast<const __m256i*>(from);
  return V4x64U(_mm256_load_si256(p));
}
template <>
HH_INLINE V8x32F Load(const V8x32F::T* const HH_RESTRICT from) {
  return V8x32F(_mm256_load_ps(from));
}
template <>
HH_INLINE V4x64F Load(const V4x64F::T* const HH_RESTRICT from) {
  return V4x64F(_mm256_load_pd(from));
}

template <>
HH_INLINE V32x8U LoadUnaligned(const V32x8U::T* const HH_RESTRICT from) {
  const __m256i* const HH_RESTRICT p = reinterpret_cast<const __m256i*>(from);
  return V32x8U(_mm256_loadu_si256(p));
}
template <>
HH_INLINE V16x16U LoadUnaligned(const V16x16U::T* const HH_RESTRICT from) {
  const __m256i* const HH_RESTRICT p = reinterpret_cast<const __m256i*>(from);
  return V16x16U(_mm256_loadu_si256(p));
}
template <>
HH_INLINE V8x32U LoadUnaligned(const V8x32U::T* const HH_RESTRICT from) {
  const __m256i* const HH_RESTRICT p = reinterpret_cast<const __m256i*>(from);
  return V8x32U(_mm256_loadu_si256(p));
}
template <>
HH_INLINE V4x64U LoadUnaligned(const V4x64U::T* const HH_RESTRICT from) {
  const __m256i* const HH_RESTRICT p = reinterpret_cast<const __m256i*>(from);
  return V4x64U(_mm256_loadu_si256(p));
}
template <>
HH_INLINE V8x32F LoadUnaligned(const V8x32F::T* const HH_RESTRICT from) {
  return V8x32F(_mm256_loadu_ps(from));
}
template <>
HH_INLINE V4x64F LoadUnaligned(const V4x64F::T* const HH_RESTRICT from) {
  return V4x64F(_mm256_loadu_pd(from));
}

// "to" must be vector-aligned.
template <typename T>
HH_INLINE void Store(const V256<T>& v, T* const HH_RESTRICT to) {
  _mm256_store_si256(reinterpret_cast<__m256i * HH_RESTRICT>(to), v);
}
HH_INLINE void Store(const V256<float>& v, float* const HH_RESTRICT to) {
  _mm256_store_ps(to, v);
}
HH_INLINE void Store(const V256<double>& v, double* const HH_RESTRICT to) {
  _mm256_store_pd(to, v);
}

template <typename T>
HH_INLINE void StoreUnaligned(const V256<T>& v, T* const HH_RESTRICT to) {
  _mm256_storeu_si256(reinterpret_cast<__m256i * HH_RESTRICT>(to), v);
}
HH_INLINE void StoreUnaligned(const V256<float>& v,
                              float* const HH_RESTRICT to) {
  _mm256_storeu_ps(to, v);
}
HH_INLINE void StoreUnaligned(const V256<double>& v,
                              double* const HH_RESTRICT to) {
  _mm256_storeu_pd(to, v);
}

// Writes directly to (aligned) memory, bypassing the cache. This is useful for
// data that will not be read again in the near future.
template <typename T>
HH_INLINE void Stream(const V256<T>& v, T* const HH_RESTRICT to) {
  _mm256_stream_si256(reinterpret_cast<__m256i * HH_RESTRICT>(to), v);
}
HH_INLINE void Stream(const V256<float>& v, float* const HH_RESTRICT to) {
  _mm256_stream_ps(to, v);
}
HH_INLINE void Stream(const V256<double>& v, double* const HH_RESTRICT to) {
  _mm256_stream_pd(to, v);
}

// Miscellaneous functions.

template <typename T>
HH_INLINE V256<T> RotateLeft(const V256<T>& v, const int count) {
  constexpr size_t num_bits = sizeof(T) * 8;
  return (v << count) | (v >> (num_bits - count));
}

template <typename T>
HH_INLINE V256<T> AndNot(const V256<T>& neg_mask, const V256<T>& values) {
  return V256<T>(_mm256_andnot_si256(neg_mask, values));
}
template <>
HH_INLINE V256<float> AndNot(const V256<float>& neg_mask,
                             const V256<float>& values) {
  return V256<float>(_mm256_andnot_ps(neg_mask, values));
}
template <>
HH_INLINE V256<double> AndNot(const V256<double>& neg_mask,
                              const V256<double>& values) {
  return V256<double>(_mm256_andnot_pd(neg_mask, values));
}

HH_INLINE V8x32F Select(const V8x32F& a, const V8x32F& b, const V8x32F& mask) {
  return V8x32F(_mm256_blendv_ps(a, b, mask));
}

HH_INLINE V4x64F Select(const V4x64F& a, const V4x64F& b, const V4x64F& mask) {
  return V4x64F(_mm256_blendv_pd(a, b, mask));
}

// Min/Max

HH_INLINE V32x8U Min(const V32x8U& v0, const V32x8U& v1) {
  return V32x8U(_mm256_min_epu8(v0, v1));
}

HH_INLINE V32x8U Max(const V32x8U& v0, const V32x8U& v1) {
  return V32x8U(_mm256_max_epu8(v0, v1));
}

HH_INLINE V16x16U Min(const V16x16U& v0, const V16x16U& v1) {
  return V16x16U(_mm256_min_epu16(v0, v1));
}

HH_INLINE V16x16U Max(const V16x16U& v0, const V16x16U& v1) {
  return V16x16U(_mm256_max_epu16(v0, v1));
}

HH_INLINE V8x32U Min(const V8x32U& v0, const V8x32U& v1) {
  return V8x32U(_mm256_min_epu32(v0, v1));
}

HH_INLINE V8x32U Max(const V8x32U& v0, const V8x32U& v1) {
  return V8x32U(_mm256_max_epu32(v0, v1));
}

HH_INLINE V8x32F Min(const V8x32F& v0, const V8x32F& v1) {
  return V8x32F(_mm256_min_ps(v0, v1));
}

HH_INLINE V8x32F Max(const V8x32F& v0, const V8x32F& v1) {
  return V8x32F(_mm256_max_ps(v0, v1));
}

HH_INLINE V4x64F Min(const V4x64F& v0, const V4x64F& v1) {
  return V4x64F(_mm256_min_pd(v0, v1));
}

HH_INLINE V4x64F Max(const V4x64F& v0, const V4x64F& v1) {
  return V4x64F(_mm256_max_pd(v0, v1));
}

}  // namespace HH_TARGET_NAME
}  // namespace highwayhash

#endif  // HH_DISABLE_TARGET_SPECIFIC
#endif  // HIGHWAYHASH_VECTOR256_H_
