#ifndef STAN_MATH_PRIM_MAT_FUN_LOG_DETERMINANT_SPD_HPP
#define STAN_MATH_PRIM_MAT_FUN_LOG_DETERMINANT_SPD_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Returns the log absolute determinant of the specified square matrix.
 *
 * @param m Specified matrix.
 * @return log absolute determinant of the matrix.
 * @throw std::domain_error if matrix is not square.
 */
template <typename T, int R, int C>
inline T log_determinant_spd(const Eigen::Matrix<T, R, C>& m) {
  using std::log;
  check_square("log_determinant_spd", "m", m);
  //      Eigen::TriangularView< Eigen::Matrix<T, R, C>, Eigen::Lower >
  //        L(m.llt().matrixL());
  //      T ret(0.0);
  //      for (size_t i = 0; i < L.rows(); i++)
  //        ret += log(L(i, i));
  //      return 2*ret;
  return m.ldlt().vectorD().array().log().sum();
}

}  // namespace math
}  // namespace stan
#endif
