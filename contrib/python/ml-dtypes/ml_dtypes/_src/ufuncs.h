/* Copyright 2022 The ml_dtypes Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#ifndef ML_DTYPES_UFUNCS_H_
#define ML_DTYPES_UFUNCS_H_

// Must be included first
// clang-format off
#include "_src/numpy.h"
// clang-format on

#include <cmath>    // NOLINT
#include <complex>  // NOLINT

#include "_src/common.h"  // NOLINT

// Some versions of MSVC define a "copysign" macro which wreaks havoc.
#if defined(_MSC_VER) && defined(copysign)
#undef copysign
#endif

namespace ml_dtypes {

template <typename InType, typename OutType, typename Functor>
struct UnaryUFunc {
  static std::vector<int> Types() {
    return {TypeDescriptor<InType>::Dtype(), TypeDescriptor<OutType>::Dtype()};
  }
  static void Call(char** args, const npy_intp* dimensions,
                   const npy_intp* steps, void* data) {
    const char* i0 = args[0];
    char* o = args[1];
    for (npy_intp k = 0; k < *dimensions; k++) {
      auto x = *reinterpret_cast<const typename TypeDescriptor<InType>::T*>(i0);
      *reinterpret_cast<typename TypeDescriptor<OutType>::T*>(o) = Functor()(x);
      i0 += steps[0];
      o += steps[1];
    }
  }
};

template <typename InType, typename OutType, typename OutType2,
          typename Functor>
struct UnaryUFunc2 {
  static std::vector<int> Types() {
    return {TypeDescriptor<InType>::Dtype(), TypeDescriptor<OutType>::Dtype(),
            TypeDescriptor<OutType2>::Dtype()};
  }
  static void Call(char** args, const npy_intp* dimensions,
                   const npy_intp* steps, void* data) {
    const char* i0 = args[0];
    char* o0 = args[1];
    char* o1 = args[2];
    for (npy_intp k = 0; k < *dimensions; k++) {
      auto x = *reinterpret_cast<const typename TypeDescriptor<InType>::T*>(i0);
      std::tie(*reinterpret_cast<typename TypeDescriptor<OutType>::T*>(o0),
               *reinterpret_cast<typename TypeDescriptor<OutType2>::T*>(o1)) =
          Functor()(x);
      i0 += steps[0];
      o0 += steps[1];
      o1 += steps[2];
    }
  }
};

template <typename InType, typename OutType, typename Functor>
struct BinaryUFunc {
  static std::vector<int> Types() {
    return {TypeDescriptor<InType>::Dtype(), TypeDescriptor<InType>::Dtype(),
            TypeDescriptor<OutType>::Dtype()};
  }
  static void Call(char** args, const npy_intp* dimensions,
                   const npy_intp* steps, void* data) {
    const char* i0 = args[0];
    const char* i1 = args[1];
    char* o = args[2];
    for (npy_intp k = 0; k < *dimensions; k++) {
      auto x = *reinterpret_cast<const typename TypeDescriptor<InType>::T*>(i0);
      auto y = *reinterpret_cast<const typename TypeDescriptor<InType>::T*>(i1);
      *reinterpret_cast<typename TypeDescriptor<OutType>::T*>(o) =
          Functor()(x, y);
      i0 += steps[0];
      i1 += steps[1];
      o += steps[2];
    }
  }
};

template <typename InType, typename InType2, typename OutType, typename Functor>
struct BinaryUFunc2 {
  static std::vector<int> Types() {
    return {TypeDescriptor<InType>::Dtype(), TypeDescriptor<InType2>::Dtype(),
            TypeDescriptor<OutType>::Dtype()};
  }
  static void Call(char** args, const npy_intp* dimensions,
                   const npy_intp* steps, void* data) {
    const char* i0 = args[0];
    const char* i1 = args[1];
    char* o = args[2];
    for (npy_intp k = 0; k < *dimensions; k++) {
      auto x = *reinterpret_cast<const typename TypeDescriptor<InType>::T*>(i0);
      auto y =
          *reinterpret_cast<const typename TypeDescriptor<InType2>::T*>(i1);
      *reinterpret_cast<typename TypeDescriptor<OutType>::T*>(o) =
          Functor()(x, y);
      i0 += steps[0];
      i1 += steps[1];
      o += steps[2];
    }
  }
};

template <typename UFunc, typename CustomT>
bool RegisterUFunc(PyObject* numpy, const char* name) {
  std::vector<int> types = UFunc::Types();
  PyUFuncGenericFunction fn =
      reinterpret_cast<PyUFuncGenericFunction>(UFunc::Call);
  Safe_PyObjectPtr ufunc_obj = make_safe(PyObject_GetAttrString(numpy, name));
  if (!ufunc_obj) {
    return false;
  }
  PyUFuncObject* ufunc = reinterpret_cast<PyUFuncObject*>(ufunc_obj.get());
  if (static_cast<int>(types.size()) != ufunc->nargs) {
    PyErr_Format(PyExc_AssertionError,
                 "ufunc %s takes %d arguments, loop takes %lu", name,
                 ufunc->nargs, types.size());
    return false;
  }
  if (PyUFunc_RegisterLoopForType(ufunc, TypeDescriptor<CustomT>::Dtype(), fn,
                                  const_cast<int*>(types.data()),
                                  nullptr) < 0) {
    return false;
  }
  return true;
}

namespace ufuncs {

template <typename T>
struct Add {
  T operator()(T a, T b) { return a + b; }
};
template <typename T>
struct Subtract {
  T operator()(T a, T b) { return a - b; }
};
template <typename T>
struct Multiply {
  T operator()(T a, T b) { return a * b; }
};
template <typename T>
struct TrueDivide {
  T operator()(T a, T b) { return a / b; }
};

inline std::pair<float, float> divmod(float a, float b) {
  if (b == 0.0f) {
    float nan = std::numeric_limits<float>::quiet_NaN();
    float inf = std::numeric_limits<float>::infinity();

    if (std::isnan(a) || (a == 0.0f)) {
      return {nan, nan};
    } else {
      return {std::signbit(a) == std::signbit(b) ? inf : -inf, nan};
    }
  }
  float mod = std::fmod(a, b);
  float div = (a - mod) / b;
  if (mod != 0.0f) {
    if ((b < 0.0f) != (mod < 0.0f)) {
      mod += b;
      div -= 1.0f;
    }
  } else {
    mod = std::copysign(0.0f, b);
  }

  float floordiv;
  if (div != 0.0f) {
    floordiv = std::floor(div);
    if (div - floordiv > 0.5f) {
      floordiv += 1.0f;
    }
  } else {
    floordiv = std::copysign(0.0f, a / b);
  }
  return {floordiv, mod};
}

template <typename T>
struct FloorDivide {
  template <typename U = T,
            std::enable_if_t<TypeDescriptor<U>::is_integral, bool> = true>
  T operator()(T x, T y) {
    if (y == T(0)) {
      PyErr_WarnEx(PyExc_RuntimeWarning,
                   "divide by zero encountered in floor_divide", 1);
      return T(0);
    }
    T v = x / y;
    if (((x > 0) != (y > 0)) && x % y != 0) {
      v = v - T(1);
    }
    return v;
  }
  template <typename U = T,
            std::enable_if_t<TypeDescriptor<U>::is_floating, bool> = true>
  T operator()(T a, T b) {
    return T(divmod(static_cast<float>(a), static_cast<float>(b)).first);
  }
};
template <typename T>
struct Remainder {
  template <typename U = T,
            std::enable_if_t<TypeDescriptor<U>::is_integral, bool> = true>
  T operator()(T x, T y) {
    if (y == 0) {
      PyErr_WarnEx(PyExc_RuntimeWarning,
                   "divide by zero encountered in remainder", 1);
      return T(0);
    }
    T v = x % y;
    if (v != 0 && ((v < 0) != (y < 0))) {
      v = v + y;
    }
    return v;
  }
  template <typename U = T,
            std::enable_if_t<TypeDescriptor<U>::is_floating, bool> = true>
  T operator()(T a, T b) {
    return T(divmod(static_cast<float>(a), static_cast<float>(b)).second);
  }
};
template <typename T>
struct DivmodUFunc {
  static std::vector<int> Types() {
    return {TypeDescriptor<T>::Dtype(), TypeDescriptor<T>::Dtype(),
            TypeDescriptor<T>::Dtype(), TypeDescriptor<T>::Dtype()};
  }
  static void Call(char** args, npy_intp* dimensions, npy_intp* steps,
                   void* data) {
    const char* i0 = args[0];
    const char* i1 = args[1];
    char* o0 = args[2];
    char* o1 = args[3];
    for (npy_intp k = 0; k < *dimensions; k++) {
      T x = *reinterpret_cast<const T*>(i0);
      T y = *reinterpret_cast<const T*>(i1);
      float floordiv, mod;
      std::tie(floordiv, mod) =
          divmod(static_cast<float>(x), static_cast<float>(y));
      *reinterpret_cast<T*>(o0) = T(floordiv);
      *reinterpret_cast<T*>(o1) = T(mod);
      i0 += steps[0];
      i1 += steps[1];
      o0 += steps[2];
      o1 += steps[3];
    }
  }
};
template <typename T>
struct Fmod {
  T operator()(T a, T b) {
    return T(std::fmod(static_cast<float>(a), static_cast<float>(b)));
  }
};
template <typename T>
struct Negative {
  T operator()(T a) { return -a; }
};
template <typename T>
struct Positive {
  T operator()(T a) { return a; }
};
template <typename T>
struct Power {
  T operator()(T a, T b) {
    return T(std::pow(static_cast<float>(a), static_cast<float>(b)));
  }
};
template <typename T>
struct Abs {
  T operator()(T a) { return Eigen::numext::abs(a); }
};
template <typename T>
struct Cbrt {
  T operator()(T a) { return T(std::cbrt(static_cast<float>(a))); }
};
template <typename T>
struct Ceil {
  T operator()(T a) { return T(std::ceil(static_cast<float>(a))); }
};

// Helper struct for getting a bit representation provided a byte size.
template <int kNumBytes>
struct GetUnsignedInteger;

template <>
struct GetUnsignedInteger<1> {
  using type = uint8_t;
};

template <>
struct GetUnsignedInteger<2> {
  using type = uint16_t;
};

template <typename T>
using BitsType = typename GetUnsignedInteger<sizeof(T)>::type;

template <typename T>
std::pair<BitsType<T>, BitsType<T>> SignAndMagnitude(T x) {
  const BitsType<T> x_bits = Eigen::numext::bit_cast<BitsType<T>>(x);
  // Unsigned floating point format (e.g. E8M0) => no sign bit (zero by
  // default).
  if constexpr (!std::numeric_limits<T>::is_signed) {
    return {BitsType<T>(0), x_bits};
  }
  // For types that represent NaN by -0, (i.e. *fnuz), abs(x) remains -0 without
  // flipping the sign. Therefore, we need to explicitly check the
  // most-significant bit.
  // For types without NaNs (i.e. mxfloat), use xor to keep the sign bit, which
  // may be not the most-significant bit.
  constexpr BitsType<T> kSignMask = BitsType<T>(1)
                                    << (sizeof(BitsType<T>) * CHAR_BIT - 1);
  constexpr bool has_nan = std::numeric_limits<T>::has_quiet_NaN;
  const BitsType<T> x_abs_bits =
      Eigen::numext::bit_cast<BitsType<T>>(Eigen::numext::abs(x));
  return {has_nan ? x_bits & kSignMask : x_bits ^ x_abs_bits, x_abs_bits};
}

template <typename T>
struct CopySign {
  T operator()(T a, T b) {
    // Unsigned floating point format => no change.
    if constexpr (!std::numeric_limits<T>::is_signed) {
      return a;
    }
    auto [a_sign, a_abs_bits] = SignAndMagnitude(a);
    auto [b_sign, b_abs_bits] = SignAndMagnitude(b);
    BitsType<T> rep = a_abs_bits | b_sign;
    return Eigen::numext::bit_cast<T>(rep);
  }
};

template <typename T>
struct Exp {
  T operator()(T a) { return T(std::exp(static_cast<float>(a))); }
};
template <typename T>
struct Exp2 {
  T operator()(T a) { return T(std::exp2(static_cast<float>(a))); }
};
template <typename T>
struct Expm1 {
  T operator()(T a) { return T(std::expm1(static_cast<float>(a))); }
};
template <typename T>
struct Floor {
  T operator()(T a) { return T(std::floor(static_cast<float>(a))); }
};
template <typename T>
struct Frexp {
  std::pair<T, int> operator()(T a) {
    int exp;
    float f = std::frexp(static_cast<float>(a), &exp);
    return {T(f), exp};
  }
};
template <typename T>
struct Heaviside {
  T operator()(T x, T h0) {
    if (Eigen::numext::isnan(x)) {
      return x;
    }
    auto [sign_x, abs_x] = SignAndMagnitude(x);
    // x == 0
    if (abs_x == 0) {
      return h0;
    }
    return sign_x ? T(0.0f) : T(1.0f);
  }
};
template <typename T>
struct Conjugate {
  T operator()(T a) { return a; }
};
template <typename T>
struct IsFinite {
  bool operator()(T a) { return Eigen::numext::isfinite(a); }
};
template <typename T>
struct IsInf {
  bool operator()(T a) { return Eigen::numext::isinf(a); }
};
template <typename T>
struct IsNan {
  bool operator()(T a) { return Eigen::numext::isnan(a); }
};
template <typename T>
struct Ldexp {
  T operator()(T a, int exp) {
    return T(std::ldexp(static_cast<float>(a), exp));
  }
};
template <typename T>
struct Log {
  T operator()(T a) { return T(std::log(static_cast<float>(a))); }
};
template <typename T>
struct Log2 {
  T operator()(T a) { return T(std::log2(static_cast<float>(a))); }
};
template <typename T>
struct Log10 {
  T operator()(T a) { return T(std::log10(static_cast<float>(a))); }
};
template <typename T>
struct Log1p {
  T operator()(T a) { return T(std::log1p(static_cast<float>(a))); }
};
template <typename T>
struct LogAddExp {
  T operator()(T bx, T by) {
    float x = static_cast<float>(bx);
    float y = static_cast<float>(by);
    if (x == y) {
      // Handles infinities of the same sign.
      return T(x + std::log(2.0f));
    }
    float out = std::numeric_limits<float>::quiet_NaN();
    if (x > y) {
      out = x + std::log1p(std::exp(y - x));
    } else if (x < y) {
      out = y + std::log1p(std::exp(x - y));
    }
    return T(out);
  }
};
template <typename T>
struct LogAddExp2 {
  T operator()(T bx, T by) {
    float x = static_cast<float>(bx);
    float y = static_cast<float>(by);
    if (x == y) {
      // Handles infinities of the same sign.
      return T(x + 1.0f);
    }
    float out = std::numeric_limits<float>::quiet_NaN();
    if (x > y) {
      out = x + std::log1p(std::exp2(y - x)) / std::log(2.0f);
    } else if (x < y) {
      out = y + std::log1p(std::exp2(x - y)) / std::log(2.0f);
    }
    return T(out);
  }
};
template <typename T>
struct Modf {
  std::pair<T, T> operator()(T a) {
    float integral;
    float f = std::modf(static_cast<float>(a), &integral);
    return {T(f), T(integral)};
  }
};

template <typename T>
struct Reciprocal {
  T operator()(T a) { return T(1.f / static_cast<float>(a)); }
};
template <typename T>
struct Rint {
  T operator()(T a) { return T(std::rint(static_cast<float>(a))); }
};
template <typename T>
struct Sign {
  T operator()(T a) {
    if (Eigen::numext::isnan(a)) {
      return a;
    }
    auto [sign_a, abs_a] = SignAndMagnitude(a);
    if (abs_a == 0) {
      return a;
    }
    return sign_a ? T(-1) : T(1);
  }
};
template <typename T>
struct SignBit {
  bool operator()(T a) {
    auto [sign_a, abs_a] = SignAndMagnitude(a);
    return sign_a;
  }
};
template <typename T>
struct Sqrt {
  T operator()(T a) { return T(std::sqrt(static_cast<float>(a))); }
};
template <typename T>
struct Square {
  T operator()(T a) {
    float f(a);
    return T(f * f);
  }
};
template <typename T>
struct Trunc {
  T operator()(T a) { return T(std::trunc(static_cast<float>(a))); }
};

// Trigonometric functions
template <typename T>
struct Sin {
  T operator()(T a) { return T(std::sin(static_cast<float>(a))); }
};
template <typename T>
struct Cos {
  T operator()(T a) { return T(std::cos(static_cast<float>(a))); }
};
template <typename T>
struct Tan {
  T operator()(T a) { return T(std::tan(static_cast<float>(a))); }
};
template <typename T>
struct Arcsin {
  T operator()(T a) { return T(std::asin(static_cast<float>(a))); }
};
template <typename T>
struct Arccos {
  T operator()(T a) { return T(std::acos(static_cast<float>(a))); }
};
template <typename T>
struct Arctan {
  T operator()(T a) { return T(std::atan(static_cast<float>(a))); }
};
template <typename T>
struct Arctan2 {
  T operator()(T a, T b) {
    return T(std::atan2(static_cast<float>(a), static_cast<float>(b)));
  }
};
template <typename T>
struct Hypot {
  T operator()(T a, T b) {
    return T(std::hypot(static_cast<float>(a), static_cast<float>(b)));
  }
};
template <typename T>
struct Sinh {
  T operator()(T a) { return T(std::sinh(static_cast<float>(a))); }
};
template <typename T>
struct Cosh {
  T operator()(T a) { return T(std::cosh(static_cast<float>(a))); }
};
template <typename T>
struct Tanh {
  T operator()(T a) { return T(std::tanh(static_cast<float>(a))); }
};
template <typename T>
struct Arcsinh {
  T operator()(T a) { return T(std::asinh(static_cast<float>(a))); }
};
template <typename T>
struct Arccosh {
  T operator()(T a) { return T(std::acosh(static_cast<float>(a))); }
};
template <typename T>
struct Arctanh {
  T operator()(T a) { return T(std::atanh(static_cast<float>(a))); }
};
template <typename T>
struct Deg2rad {
  T operator()(T a) {
    static constexpr float radians_per_degree = M_PI / 180.0f;
    return T(static_cast<float>(a) * radians_per_degree);
  }
};
template <typename T>
struct Rad2deg {
  T operator()(T a) {
    static constexpr float degrees_per_radian = 180.0f / M_PI;
    return T(static_cast<float>(a) * degrees_per_radian);
  }
};

template <typename T>
struct Eq {
  npy_bool operator()(T a, T b) { return a == b; }
};
template <typename T>
struct Ne {
  npy_bool operator()(T a, T b) { return a != b; }
};
template <typename T>
struct Lt {
  npy_bool operator()(T a, T b) { return a < b; }
};
template <typename T>
struct Gt {
  npy_bool operator()(T a, T b) { return a > b; }
};
template <typename T>
struct Le {
  npy_bool operator()(T a, T b) { return a <= b; }
};
template <typename T>
struct Ge {
  npy_bool operator()(T a, T b) { return a >= b; }
};
template <typename T>
struct Maximum {
  T operator()(T a, T b) {
    float fa(a), fb(b);
    return Eigen::numext::isnan(fa) || fa > fb ? a : b;
  }
};
template <typename T>
struct Minimum {
  T operator()(T a, T b) {
    float fa(a), fb(b);
    return Eigen::numext::isnan(fa) || fa < fb ? a : b;
  }
};
template <typename T>
struct Fmax {
  T operator()(T a, T b) {
    float fa(a), fb(b);
    return Eigen::numext::isnan(fb) || fa > fb ? a : b;
  }
};
template <typename T>
struct Fmin {
  T operator()(T a, T b) {
    float fa(a), fb(b);
    return Eigen::numext::isnan(fb) || fa < fb ? a : b;
  }
};

template <typename T>
struct LogicalNot {
  npy_bool operator()(T a) { return !static_cast<bool>(a); }
};
template <typename T>
struct LogicalAnd {
  npy_bool operator()(T a, T b) {
    return static_cast<bool>(a) && static_cast<bool>(b);
  }
};
template <typename T>
struct LogicalOr {
  npy_bool operator()(T a, T b) {
    return static_cast<bool>(a) || static_cast<bool>(b);
  }
};
template <typename T>
struct LogicalXor {
  npy_bool operator()(T a, T b) {
    return static_cast<bool>(a) ^ static_cast<bool>(b);
  }
};

template <typename T>
struct NextAfter {
  T operator()(T from, T to) {
    BitsType<T> from_rep = Eigen::numext::bit_cast<BitsType<T>>(from);
    BitsType<T> to_rep = Eigen::numext::bit_cast<BitsType<T>>(to);
    if (Eigen::numext::isnan(from) || Eigen::numext::isnan(to)) {
      return std::numeric_limits<T>::quiet_NaN();
    }
    if (from_rep == to_rep) {
      return to;
    }
    auto [from_sign, from_abs] = SignAndMagnitude(from);
    auto [to_sign, to_abs] = SignAndMagnitude(to);
    if (from_abs == 0) {
      if (to_abs == 0) {
        return to;
      } else {
        // Smallest subnormal signed like `to`.
        return Eigen::numext::bit_cast<T>(
            static_cast<BitsType<T>>(0x01 | to_sign));
      }
    }
    BitsType<T> magnitude_adjustment =
        (from_abs > to_abs || from_sign != to_sign)
            ? static_cast<BitsType<T>>(-1)
            : static_cast<BitsType<T>>(1);
    BitsType<T> out_int = from_rep + magnitude_adjustment;
    T out = Eigen::numext::bit_cast<T>(out_int);
    // Some non-IEEE compatible formats may have a representation for NaN
    // instead of -0, ensure we return a zero in such cases.
    if constexpr (!std::numeric_limits<T>::is_iec559) {
      if (Eigen::numext::isnan(out)) {
        return Eigen::numext::bit_cast<T>(BitsType<T>{0});
      }
    }
    return out;
  }
};

template <typename T>
struct Spacing {
  T operator()(T x) {
    CopySign<T> copysign;
    if constexpr (!std::numeric_limits<T>::has_infinity) {
      if (Eigen::numext::abs(x) == std::numeric_limits<T>::max()) {
        if constexpr (!std::numeric_limits<T>::has_quiet_NaN) return T();
        return copysign(std::numeric_limits<T>::quiet_NaN(), x);
      }
    }
    // Compute the distance between the input and the next number with greater
    // magnitude. The result should have the sign of the input.
    T away = std::numeric_limits<T>::has_infinity
                 ? std::numeric_limits<T>::infinity()
                 : std::numeric_limits<T>::max();
    away = copysign(away, x);
    return NextAfter<T>()(x, away) - x;
  }
};

}  // namespace ufuncs
}  // namespace ml_dtypes

#endif  // ML_DTYPES_UFUNCS_H_
