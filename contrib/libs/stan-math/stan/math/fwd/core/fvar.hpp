#ifndef STAN_MATH_FWD_CORE_FVAR_HPP
#define STAN_MATH_FWD_CORE_FVAR_HPP

#include <stan/math/prim/scal/meta/likely.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <stan/math/fwd/scal/meta/ad_promotable.hpp>
#include <ostream>

namespace stan {
namespace math {

/**
 * This template class represents scalars used in forward-mode
 * automatic differentiation, which consist of values and
 * directional derivatives of the specified template type.  When
 * performing operations on instances of this class, all operands
 * should be either primitive integer or double values or dual
 * numbers representing derivatives in the same direction.  The
 * typical use case is to have a unit length directional
 * derivative in the direction of a single independent variable.
 *
 * By using reverse-mode automatic derivative variables,
 * second-order derivatives may
 * be calculated.  By using fvar&lt;<var&gt; instances,
 * third-order derivatives may be calculated.  These are called
 * mixed-mode automatic differentiation variable in Stan.
 *
 * Specialized functionals that perform differentiation on
 * functors may be found in the matrix subdirectories of the
 * reverse, forward, and mixed-mode directories.
 *
 * The <a
 * href="https://en.wikipedia.org/wiki/Automatic_differentiation">Wikipedia
 * page on automatic differentiation</a> describes how
 * forward-mode automatic differentiation works mathematically in
 * terms of dual numbers.
 *
 * @tparam T type of value and tangent
 */
template <typename T>
struct fvar {
  /**
   * The value of this variable.
   */
  T val_;

  /**
   * The tangent (derivative) of this variable.
   */
  T d_;

  /**
   * Return the value of this variable.
   *
   * @return value of this variable
   */
  T val() const { return val_; }

  /**
   * Return the tangent (derivative) of this variable.
   *
   * @return tangent of this variable
   */
  T tangent() const { return d_; }

  /**
   * Construct a forward variable with zero value and tangent.
   */
  fvar() : val_(0.0), d_(0.0) {}

  /**
   * Construct a forward variable with value and tangent set to
   * the value and tangent of the specified variable.
   *
   * @param[in] x variable to be copied
   */
  fvar(const fvar<T>& x) : val_(x.val_), d_(x.d_) {}

  /**
   * Construct a forward variable with the specified value and
   * zero tangent.
   *
   * @tparam V type of value (must be assignable to the value and
   *   tangent type T)
   * @param[in] v value
   */
  fvar(const T& v) : val_(v), d_(0.0) {  // NOLINT(runtime/explicit)
    if (unlikely(is_nan(v)))
      d_ = v;
  }

  /**
   * Construct a forward variable with the specified value and
   * zero tangent.
   *
   * @tparam V type of value (must be assignable to the value and
   *   tangent type T)
   * @param[in] v value
   * @param[in] dummy value given by default with enable-if
   *   metaprogramming
   */
  template <typename V>
  fvar(const V& v,
       typename std::enable_if<ad_promotable<V, T>::value>::type* dummy = 0)
      : val_(v), d_(0.0) {
    if (unlikely(is_nan(v)))
      d_ = v;
  }

  /**
   * Construct a forward variable with the specified value and
   * tangent.
   *
   * @tparam V type of value (must be assignable to the value and
   *   tangent type T)
   * @tparam D type of tangent (must be assignable to the value and
   *   tangent type T)
   * @param[in] v value
   * @param[in] d tangent
   */
  template <typename V, typename D>
  fvar(const V& v, const D& d) : val_(v), d_(d) {
    if (unlikely(is_nan(v)))
      d_ = v;
  }

  /**
   * Add the specified variable to this variable and return a
   * reference to this variable.
   *
   * @param[in] x2 variable to add
   * @return reference to this variable after addition
   */
  inline fvar<T>& operator+=(const fvar<T>& x2) {
    val_ += x2.val_;
    d_ += x2.d_;
    return *this;
  }

  /**
   * Add the specified value to this variable and return a
   * reference to this variable.
   *
   * @param[in] x2 value to add
   * @return reference to this variable after addition
   */
  inline fvar<T>& operator+=(double x2) {
    val_ += x2;
    return *this;
  }

  /**
   * Subtract the specified variable from this variable and return a
   * reference to this variable.
   *
   * @param[in] x2 variable to subtract
   * @return reference to this variable after subtraction
   */
  inline fvar<T>& operator-=(const fvar<T>& x2) {
    val_ -= x2.val_;
    d_ -= x2.d_;
    return *this;
  }

  /**
   * Subtract the specified value from this variable and return a
   * reference to this variable.
   *
   * @param[in] x2 value to add
   * @return reference to this variable after subtraction
   */
  inline fvar<T>& operator-=(double x2) {
    val_ -= x2;
    return *this;
  }

  /**
   * Multiply this variable by the the specified variable and
   * return a reference to this variable.
   *
   * @param[in] x2 variable to multiply
   * @return reference to this variable after multiplication
   */
  inline fvar<T>& operator*=(const fvar<T>& x2) {
    d_ = d_ * x2.val_ + val_ * x2.d_;
    val_ *= x2.val_;
    return *this;
  }

  /**
   * Multiply this variable by the the specified value and
   * return a reference to this variable.
   *
   * @param[in] x2 value to multiply
   * @return reference to this variable after multiplication
   */
  inline fvar<T>& operator*=(double x2) {
    val_ *= x2;
    d_ *= x2;
    return *this;
  }

  /**
   * Divide this variable by the the specified variable and
   * return a reference to this variable.
   *
   * @param[in] x2 variable to divide this variable by
   * @return reference to this variable after division
   */
  inline fvar<T>& operator/=(const fvar<T>& x2) {
    d_ = (d_ * x2.val_ - val_ * x2.d_) / (x2.val_ * x2.val_);
    val_ /= x2.val_;
    return *this;
  }

  /**
   * Divide this value by the the specified variable and
   * return a reference to this variable.
   *
   * @param[in] x2 value to divide this variable by
   * @return reference to this variable after division
   */
  inline fvar<T>& operator/=(double x2) {
    val_ /= x2;
    d_ /= x2;
    return *this;
  }

  /**
   * Increment this variable by one and return a reference to this
   * variable after the increment.
   *
   * @return reference to this variable after increment
   */
  inline fvar<T>& operator++() {
    ++val_;
    return *this;
  }

  /**
   * Increment this variable by one and return a reference to a
   * copy of this variable before it was incremented.
   *
   * @return reference to copy of this variable before increment
   */
  inline fvar<T> operator++(int /*dummy*/) {
    fvar<T> result(val_, d_);
    ++val_;
    return result;
  }

  /**
   * Decrement this variable by one and return a reference to this
   * variable after the decrement.
   *
   * @return reference to this variable after decrement
   */
  inline fvar<T>& operator--() {
    --val_;
    return *this;
  }

  /**
   * Decrement this variable by one and return a reference to a
   * copy of this variable before it was decremented.
   *
   * @return reference to copy of this variable before decrement
   */
  inline fvar<T> operator--(int /*dummy*/) {
    fvar<T> result(val_, d_);
    --val_;
    return result;
  }

  /**
   * Write the value of the specified variable to the specified
   * output stream, returning a reference to the output stream.
   *
   * @param[in,out] os stream for writing value
   * @param[in] v variable whose value is written
   * @return reference to the specified output stream
   */
  friend std::ostream& operator<<(std::ostream& os, const fvar<T>& v) {
    return os << v.val_;
  }
};
}  // namespace math
}  // namespace stan
#endif
