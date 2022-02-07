// Copyright 2017 Google Inc. All Rights Reserved.
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

#ifndef HIGHWAYHASH_SCALAR_H_
#define HIGHWAYHASH_SCALAR_H_

// WARNING: this is a "restricted" header because it is included from
// translation units compiled with different flags. This header and its
// dependencies must not define any function unless it is static inline and/or
// within namespace HH_TARGET_NAME. See arch_specific.h for details.

#include <stddef.h>  // size_t
#include <stdint.h>

#include "highwayhash/arch_specific.h"
#include "highwayhash/compiler_specific.h"

namespace highwayhash {
// To prevent ODR violations when including this from multiple translation
// units (TU) that are compiled with different flags, the contents must reside
// in a namespace whose name is unique to the TU. NOTE: this behavior is
// incompatible with precompiled modules and requires textual inclusion instead.
namespace HH_TARGET_NAME {

// Single-lane "vector" type with the same interface as V128/Scalar. Allows the
// same client template to generate both SIMD and portable code.
template <typename Type>
class Scalar {
 public:
  struct Intrinsic {
    Type t;
  };

  using T = Type;
  static constexpr size_t N = 1;

  // Leaves v_ uninitialized - typically used for output parameters.
  HH_INLINE Scalar() {}

  HH_INLINE explicit Scalar(const T t) : v_(t) {}

  HH_INLINE Scalar(const Scalar<T>& other) : v_(other.v_) {}

  HH_INLINE Scalar& operator=(const Scalar<T>& other) {
    v_ = other.v_;
    return *this;
  }

  // Convert from/to intrinsics.
  HH_INLINE Scalar(const Intrinsic& v) : v_(v.t) {}
  HH_INLINE Scalar& operator=(const Intrinsic& v) {
    v_ = v.t;
    return *this;
  }
  HH_INLINE operator Intrinsic() const { return {v_}; }

  HH_INLINE Scalar operator==(const Scalar& other) const {
    Scalar eq;
    eq.FillWithByte(v_ == other.v_ ? 0xFF : 0x00);
    return eq;
  }
  HH_INLINE Scalar operator<(const Scalar& other) const {
    Scalar lt;
    lt.FillWithByte(v_ < other.v_ ? 0xFF : 0x00);
    return lt;
  }
  HH_INLINE Scalar operator>(const Scalar& other) const {
    Scalar gt;
    gt.FillWithByte(v_ > other.v_ ? 0xFF : 0x00);
    return gt;
  }

  HH_INLINE Scalar& operator*=(const Scalar& other) {
    v_ *= other.v_;
    return *this;
  }
  HH_INLINE Scalar& operator/=(const Scalar& other) {
    v_ /= other.v_;
    return *this;
  }
  HH_INLINE Scalar& operator+=(const Scalar& other) {
    v_ += other.v_;
    return *this;
  }
  HH_INLINE Scalar& operator-=(const Scalar& other) {
    v_ -= other.v_;
    return *this;
  }

  HH_INLINE Scalar& operator&=(const Scalar& other) {
    v_ &= other.v_;
    return *this;
  }
  HH_INLINE Scalar& operator|=(const Scalar& other) {
    v_ |= other.v_;
    return *this;
  }
  HH_INLINE Scalar& operator^=(const Scalar& other) {
    v_ ^= other.v_;
    return *this;
  }

  HH_INLINE Scalar& operator<<=(const int count) {
    // In C, int64_t << 64 is undefined, but we want to match the sensible
    // behavior of SSE2 (zeroing).
    if (count >= sizeof(T) * 8) {
      v_ = 0;
    } else {
      v_ <<= count;
    }
    return *this;
  }

  HH_INLINE Scalar& operator>>=(const int count) {
    if (count >= sizeof(T) * 8) {
      v_ = 0;
    } else {
      v_ >>= count;
    }
    return *this;
  }

  // For internal use only. We need to avoid memcpy/memset because this is a
  // restricted header.
  void FillWithByte(const unsigned char value) {
    unsigned char* bytes = reinterpret_cast<unsigned char*>(&v_);
    for (size_t i = 0; i < sizeof(T); ++i) {
      bytes[i] = value;
    }
  }

  void CopyTo(unsigned char* HH_RESTRICT to_bytes) const {
    const unsigned char* from_bytes =
        reinterpret_cast<const unsigned char*>(&v_);
    for (size_t i = 0; i < sizeof(T); ++i) {
      to_bytes[i] = from_bytes[i];
    }
  }

 private:
  T v_;
};

// Non-member operators.

template <typename T>
HH_INLINE Scalar<T> operator*(const Scalar<T>& left, const Scalar<T>& right) {
  Scalar<T> t(left);
  return t *= right;
}

template <typename T>
HH_INLINE Scalar<T> operator/(const Scalar<T>& left, const Scalar<T>& right) {
  Scalar<T> t(left);
  return t /= right;
}

template <typename T>
HH_INLINE Scalar<T> operator+(const Scalar<T>& left, const Scalar<T>& right) {
  Scalar<T> t(left);
  return t += right;
}

template <typename T>
HH_INLINE Scalar<T> operator-(const Scalar<T>& left, const Scalar<T>& right) {
  Scalar<T> t(left);
  return t -= right;
}

template <typename T>
HH_INLINE Scalar<T> operator&(const Scalar<T>& left, const Scalar<T>& right) {
  Scalar<T> t(left);
  return t &= right;
}

template <typename T>
HH_INLINE Scalar<T> operator|(const Scalar<T> left, const Scalar<T>& right) {
  Scalar<T> t(left);
  return t |= right;
}

template <typename T>
HH_INLINE Scalar<T> operator^(const Scalar<T>& left, const Scalar<T>& right) {
  Scalar<T> t(left);
  return t ^= right;
}

template <typename T>
HH_INLINE Scalar<T> operator<<(const Scalar<T>& v, const int count) {
  Scalar<T> t(v);
  return t <<= count;
}

template <typename T>
HH_INLINE Scalar<T> operator>>(const Scalar<T>& v, const int count) {
  Scalar<T> t(v);
  return t >>= count;
}

using V1x8U = Scalar<uint8_t>;
using V1x16U = Scalar<uint16_t>;
using V1x16I = Scalar<int16_t>;
using V1x32U = Scalar<uint32_t>;
using V1x32I = Scalar<int32_t>;
using V1x64U = Scalar<uint64_t>;
using V1x32F = Scalar<float>;
using V1x64F = Scalar<double>;

// Load/Store.

// We differentiate between targets' vector types via template specialization.
// Calling Load<V>(floats) is more natural than Load(V8x32F(), floats) and may
// generate better code in unoptimized builds. Only declare the primary
// templates to avoid needing mutual exclusion with vector128/256.
template <class V>
HH_INLINE V Load(const typename V::T* const HH_RESTRICT from);
template <class V>
HH_INLINE V LoadUnaligned(const typename V::T* const HH_RESTRICT from);

template <>
HH_INLINE V1x8U Load<V1x8U>(const V1x8U::T* const HH_RESTRICT from) {
  return V1x8U(*from);
}
template <>
HH_INLINE V1x16U Load<V1x16U>(const V1x16U::T* const HH_RESTRICT from) {
  return V1x16U(*from);
}
template <>
HH_INLINE V1x16I Load<V1x16I>(const V1x16I::T* const HH_RESTRICT from) {
  return V1x16I(*from);
}
template <>
HH_INLINE V1x32U Load<V1x32U>(const V1x32U::T* const HH_RESTRICT from) {
  return V1x32U(*from);
}
template <>
HH_INLINE V1x32I Load<V1x32I>(const V1x32I::T* const HH_RESTRICT from) {
  return V1x32I(*from);
}
template <>
HH_INLINE V1x64U Load<V1x64U>(const V1x64U::T* const HH_RESTRICT from) {
  return V1x64U(*from);
}
template <>
HH_INLINE V1x32F Load<V1x32F>(const V1x32F::T* const HH_RESTRICT from) {
  return V1x32F(*from);
}
template <>
HH_INLINE V1x64F Load<V1x64F>(const V1x64F::T* const HH_RESTRICT from) {
  return V1x64F(*from);
}

template <>
HH_INLINE V1x8U LoadUnaligned<V1x8U>(const V1x8U::T* const HH_RESTRICT from) {
  return V1x8U(*from);
}
template <>
HH_INLINE V1x16U
LoadUnaligned<V1x16U>(const V1x16U::T* const HH_RESTRICT from) {
  return V1x16U(*from);
}
template <>
HH_INLINE V1x16I
LoadUnaligned<V1x16I>(const V1x16I::T* const HH_RESTRICT from) {
  return V1x16I(*from);
}
template <>
HH_INLINE V1x32U
LoadUnaligned<V1x32U>(const V1x32U::T* const HH_RESTRICT from) {
  return V1x32U(*from);
}
template <>
HH_INLINE V1x32I
LoadUnaligned<V1x32I>(const V1x32I::T* const HH_RESTRICT from) {
  return V1x32I(*from);
}
template <>
HH_INLINE V1x64U
LoadUnaligned<V1x64U>(const V1x64U::T* const HH_RESTRICT from) {
  return V1x64U(*from);
}
template <>
HH_INLINE V1x32F
LoadUnaligned<V1x32F>(const V1x32F::T* const HH_RESTRICT from) {
  return V1x32F(*from);
}
template <>
HH_INLINE V1x64F
LoadUnaligned<V1x64F>(const V1x64F::T* const HH_RESTRICT from) {
  return V1x64F(*from);
}

template <typename T>
HH_INLINE void Store(const Scalar<T>& v, T* const HH_RESTRICT to) {
  v.CopyTo(reinterpret_cast<unsigned char*>(to));
}

template <typename T>
HH_INLINE void StoreUnaligned(const Scalar<T>& v, T* const HH_RESTRICT to) {
  v.CopyTo(reinterpret_cast<unsigned char*>(to));
}

template <typename T>
HH_INLINE void Stream(const Scalar<T>& v, T* const HH_RESTRICT to) {
  v.CopyTo(reinterpret_cast<unsigned char*>(to));
}

// Miscellaneous functions.

template <typename T>
HH_INLINE Scalar<T> RotateLeft(const Scalar<T>& v, const int count) {
  constexpr size_t num_bits = sizeof(T) * 8;
  return (v << count) | (v >> (num_bits - count));
}

template <typename T>
HH_INLINE Scalar<T> AndNot(const Scalar<T>& neg_mask, const Scalar<T>& values) {
  return values & ~neg_mask;
}

template <typename T>
HH_INLINE Scalar<T> Select(const Scalar<T>& a, const Scalar<T>& b,
                           const Scalar<T>& mask) {
  const char* mask_bytes = reinterpret_cast<const char*>(&mask);
  return (mask_bytes[sizeof(T) - 1] & 0x80) ? b : a;
}

template <typename T>
HH_INLINE Scalar<T> Min(const Scalar<T>& v0, const Scalar<T>& v1) {
  return (v0 < v1) ? v0 : v1;
}

template <typename T>
HH_INLINE Scalar<T> Max(const Scalar<T>& v0, const Scalar<T>& v1) {
  return (v0 < v1) ? v1 : v0;
}

}  // namespace HH_TARGET_NAME
}  // namespace highwayhash

#endif  // HIGHWAYHASH_SCALAR_H_
