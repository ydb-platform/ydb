#ifndef STAN_MATH_FWD_SCAL_FUN_FDIM_HPP
#define STAN_MATH_FWD_SCAL_FUN_FDIM_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/fdim.hpp>

namespace stan {
namespace math {

/**
 * Return the positive difference of the specified values (C++11).
 *
 * @tparam T Scalar type of autodiff variables.
 * @param x First argument.
 * @param y Second argument.
 * @return Return the differences of the arguments if it is
 * positive and 0 otherwise.
 */
template <typename T>
inline fvar<T> fdim(const fvar<T>& x, const fvar<T>& y) {
  if (x.val_ < y.val_)
    return fvar<T>(fdim(x.val_, y.val_), 0);
  else
    return fvar<T>(fdim(x.val_, y.val_), x.d_ - y.d_);
}

/**
 * Return the positive difference of the specified values (C++11).
 *
 * @tparam T Scalar type of autodiff variables.
 * @param x First argument.
 * @param y Second argument.
 * @return Return the differences of the arguments if it is
 * positive and 0 otherwise.
 */
template <typename T>
inline fvar<T> fdim(const fvar<T>& x, double y) {
  if (x.val_ < y)
    return fvar<T>(fdim(x.val_, y), 0);
  else
    return fvar<T>(fdim(x.val_, y), x.d_);
}

/**
 * Return the positive difference of the specified values (C++11).
 *
 * @tparam T Scalar type of autodiff variables.
 * @param x First argument.
 * @param y Second argument.
 * @return Return the differences of the arguments if it is
 * positive and 0 otherwise.
 */
template <typename T>
inline fvar<T> fdim(double x, const fvar<T>& y) {
  if (x < y.val_)
    return fvar<T>(fdim(x, y.val_), 0);
  else
    return fvar<T>(fdim(x, y.val_), -y.d_);
}

}  // namespace math
}  // namespace stan
#endif
