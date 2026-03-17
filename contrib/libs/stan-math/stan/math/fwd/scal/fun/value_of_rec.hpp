#ifndef STAN_MATH_FWD_SCAL_FUN_VALUE_OF_REC_HPP
#define STAN_MATH_FWD_SCAL_FUN_VALUE_OF_REC_HPP

#include <stan/math/prim/scal/fun/value_of_rec.hpp>
#include <stan/math/fwd/core.hpp>

namespace stan {
namespace math {

/**
 * Return the value of the specified variable.
 *
 * T must implement value_of_rec.
 *
 * @tparam T Scalar type
 * @param v Variable.
 * @return Value of variable.
 */

template <typename T>
inline double value_of_rec(const fvar<T>& v) {
  return value_of_rec(v.val_);
}

}  // namespace math
}  // namespace stan
#endif
