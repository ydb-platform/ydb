#ifndef STAN_MATH_FWD_SCAL_FUN_HYPOT_HPP
#define STAN_MATH_FWD_SCAL_FUN_HYPOT_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/hypot.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Return the length of the hypoteneuse of a right triangle with
 * opposite and adjacent side lengths given by the specified
 * arguments (C++11).  In symbols, if the arguments are
 * <code>1</code> and <code>x2</code>, the result is <code>sqrt(x1 *
 * x1 + x2 * x2)</code>.
 *
 * @tparam T Scalar type of autodiff variables.
 * @param x1 First argument.
 * @param x2 Second argument.
 * @return Length of hypoteneuse of right triangle with opposite
 * and adjacent side lengths x1 and x2.
 */
template <typename T>
inline fvar<T> hypot(const fvar<T>& x1, const fvar<T>& x2) {
  using std::sqrt;
  T u = hypot(x1.val_, x2.val_);
  return fvar<T>(u, (x1.d_ * x1.val_ + x2.d_ * x2.val_) / u);
}

/**
 * Return the length of the hypoteneuse of a right triangle with
 * opposite and adjacent side lengths given by the specified
 * arguments (C++11).  In symbols, if the arguments are
 * <code>1</code> and <code>x2</code>, the result is <code>sqrt(x1 *
 * x1 + x2 * x2)</code>.
 *
 * @tparam T Scalar type of autodiff variable.
 * @param x1 First argument.
 * @param x2 Second argument.
 * @return Length of hypoteneuse of right triangle with opposite
 * and adjacent side lengths x1 and x2.
 */
template <typename T>
inline fvar<T> hypot(const fvar<T>& x1, double x2) {
  using std::sqrt;
  T u = hypot(x1.val_, x2);
  return fvar<T>(u, (x1.d_ * x1.val_) / u);
}

/**
 * Return the length of the hypoteneuse of a right triangle with
 * opposite and adjacent side lengths given by the specified
 * arguments (C++11).  In symbols, if the arguments are
 * <code>1</code> and <code>x2</code>, the result is <code>sqrt(x1 *
 * x1 + x2 * x2)</code>.
 *
 * @tparam T Scalar type of autodiff variable.
 * @param x1 First argument.
 * @param x2 Second argument.
 * @return Length of hypoteneuse of right triangle with opposite
 * and adjacent side lengths x1 and x2.
 */
template <typename T>
inline fvar<T> hypot(double x1, const fvar<T>& x2) {
  using std::sqrt;
  T u = hypot(x1, x2.val_);
  return fvar<T>(u, (x2.d_ * x2.val_) / u);
}

}  // namespace math
}  // namespace stan
#endif
