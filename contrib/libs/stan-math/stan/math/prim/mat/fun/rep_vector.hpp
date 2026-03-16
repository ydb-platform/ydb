#ifndef STAN_MATH_PRIM_MAT_FUN_REP_VECTOR_HPP
#define STAN_MATH_PRIM_MAT_FUN_REP_VECTOR_HPP

#include <boost/math/tools/promotion.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>

namespace stan {
namespace math {

template <typename T>
inline Eigen::Matrix<typename boost::math::tools::promote_args<T>::type,
                     Eigen::Dynamic, 1>
rep_vector(const T& x, int n) {
  check_nonnegative("rep_vector", "n", n);
  return Eigen::Matrix<typename boost::math::tools::promote_args<T>::type,
                       Eigen::Dynamic, 1>::Constant(n, x);
}

}  // namespace math
}  // namespace stan

#endif
