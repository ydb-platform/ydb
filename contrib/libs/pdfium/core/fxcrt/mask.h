// Copyright 2021 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef CORE_FXCRT_MASK_H_
#define CORE_FXCRT_MASK_H_

#include <type_traits>

namespace fxcrt {

// Provides extremely strict type-checking on masks of enum class bitflags,
// for code where flags may not be passed consistently.
template <typename E>
class Mask {
 public:
  using UnderlyingType = typename std::underlying_type<E>::type;

  // Escape hatch for when value comes aross an API, say.
  static Mask FromUnderlyingUnchecked(UnderlyingType val) { return Mask(val); }

  constexpr Mask() = default;
  constexpr Mask(const Mask& that) = default;

  // NOLINTNEXTLINE(runtime/explicit)
  constexpr Mask(E val) : val_(static_cast<UnderlyingType>(val)) {}

  // Unfortunately, std::initializer_list<> can't be used in constexpr
  // methods per C++ standards, and we need constexpr for a zero-cost
  // abstraction.  Hence, expand out constructors of various arity.
  constexpr Mask(E v1, E v2)
      : val_(static_cast<UnderlyingType>(v1) |
             static_cast<UnderlyingType>(v2)) {}

  constexpr Mask(E v1, E v2, E v3)
      : val_(static_cast<UnderlyingType>(v1) | static_cast<UnderlyingType>(v2) |
             static_cast<UnderlyingType>(v3)) {}

  constexpr Mask(E v1, E v2, E v3, E v4)
      : val_(static_cast<UnderlyingType>(v1) | static_cast<UnderlyingType>(v2) |
             static_cast<UnderlyingType>(v3) |
             static_cast<UnderlyingType>(v4)) {}

  constexpr Mask(E v1, E v2, E v3, E v4, E v5)
      : val_(static_cast<UnderlyingType>(v1) | static_cast<UnderlyingType>(v2) |
             static_cast<UnderlyingType>(v3) | static_cast<UnderlyingType>(v4) |
             static_cast<UnderlyingType>(v5)) {}

  constexpr Mask(E v1, E v2, E v3, E v4, E v5, E v6)
      : val_(static_cast<UnderlyingType>(v1) | static_cast<UnderlyingType>(v2) |
             static_cast<UnderlyingType>(v3) | static_cast<UnderlyingType>(v4) |
             static_cast<UnderlyingType>(v5) |
             static_cast<UnderlyingType>(v6)) {}

  constexpr Mask(E v1, E v2, E v3, E v4, E v5, E v6, E v7)
      : val_(static_cast<UnderlyingType>(v1) | static_cast<UnderlyingType>(v2) |
             static_cast<UnderlyingType>(v3) | static_cast<UnderlyingType>(v4) |
             static_cast<UnderlyingType>(v5) | static_cast<UnderlyingType>(v6) |
             static_cast<UnderlyingType>(v7)) {}

  constexpr Mask(E v1, E v2, E v3, E v4, E v5, E v6, E v7, E v8)
      : val_(static_cast<UnderlyingType>(v1) | static_cast<UnderlyingType>(v2) |
             static_cast<UnderlyingType>(v3) | static_cast<UnderlyingType>(v4) |
             static_cast<UnderlyingType>(v5) | static_cast<UnderlyingType>(v6) |
             static_cast<UnderlyingType>(v7) |
             static_cast<UnderlyingType>(v8)) {}

  explicit operator bool() const { return !!val_; }
  Mask operator~() const { return Mask(~val_); }
  constexpr Mask operator|(const Mask& that) const {
    return Mask(val_ | that.val_);
  }
  constexpr Mask operator&(const Mask& that) const {
    return Mask(val_ & that.val_);
  }
  constexpr Mask operator^(const Mask& that) const {
    return Mask(val_ ^ that.val_);
  }
  Mask& operator=(const Mask& that) {
    val_ = that.val_;
    return *this;
  }
  Mask& operator|=(const Mask& that) {
    val_ |= that.val_;
    return *this;
  }
  Mask& operator&=(const Mask& that) {
    val_ &= that.val_;
    return *this;
  }
  Mask& operator^=(const Mask& that) {
    val_ ^= that.val_;
    return *this;
  }
  bool operator==(const Mask& that) const { return val_ == that.val_; }
  bool operator!=(const Mask& that) const { return val_ != that.val_; }

  bool TestAll(const Mask& that) const {
    return (val_ & that.val_) == that.val_;
  }

  // Because ~ can't be applied to enum class without casting.
  void Clear(const Mask& that) { val_ &= ~that.val_; }

  // Escape hatch, usage should be minimized.
  UnderlyingType UncheckedValue() const { return val_; }

 private:
  explicit constexpr Mask(UnderlyingType val) : val_(val) {}

  UnderlyingType val_ = 0;
};

}  // namespace fxcrt

using fxcrt::Mask;

#endif  // CORE_FXCRT_MASK_H_
