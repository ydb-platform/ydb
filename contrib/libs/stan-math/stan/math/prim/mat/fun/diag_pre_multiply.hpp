#ifndef STAN_MATH_PRIM_MAT_FUN_DIAG_PRE_MULTIPLY_HPP
#define STAN_MATH_PRIM_MAT_FUN_DIAG_PRE_MULTIPLY_HPP

#include <stan/math/prim/mat/err/check_vector.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {

template <typename T1, typename T2, int R1, int C1, int R2, int C2>
Eigen::Matrix<typename boost::math::tools::promote_args<T1, T2>::type, R2, C2>
diag_pre_multiply(const Eigen::Matrix<T1, R1, C1>& m1,
                  const Eigen::Matrix<T2, R2, C2>& m2) {
  check_vector("diag_pre_multiply", "m1", m1);
  check_size_match("diag_pre_multiply", "m1.size()", m1.size(), "m2.rows()",
                   m2.rows());
  return m1.asDiagonal() * m2;
}

}  // namespace math
}  // namespace stan
#endif
