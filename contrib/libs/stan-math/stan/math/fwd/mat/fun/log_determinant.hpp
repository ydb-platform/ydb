#ifndef STAN_MATH_FWD_MAT_FUN_LOG_DETERMINANT_HPP
#define STAN_MATH_FWD_MAT_FUN_LOG_DETERMINANT_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/fwd/core.hpp>
#include <stan/math/fwd/mat/fun/typedefs.hpp>
#include <stan/math/fwd/mat/fun/determinant.hpp>
#include <stan/math/fwd/scal/fun/fabs.hpp>
#include <stan/math/fwd/scal/fun/log.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <boost/math/tools/promotion.hpp>
#include <vector>

namespace stan {
namespace math {

template <typename T, int R, int C>
inline fvar<T> log_determinant(const Eigen::Matrix<fvar<T>, R, C>& m) {
  check_square("log_determinant", "m", m);

  return log(fabs(determinant(m)));
}

}  // namespace math
}  // namespace stan
#endif
