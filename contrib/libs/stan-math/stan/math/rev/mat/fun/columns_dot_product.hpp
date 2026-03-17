#ifndef STAN_MATH_REV_MAT_FUN_COLUMNS_DOT_PRODUCT_HPP
#define STAN_MATH_REV_MAT_FUN_COLUMNS_DOT_PRODUCT_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/err/check_vector.hpp>
#include <stan/math/prim/arr/err/check_matching_sizes.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/rev/core.hpp>
#include <stan/math/rev/mat/fun/typedefs.hpp>
#include <stan/math/rev/scal/fun/value_of.hpp>
#include <stan/math/rev/mat/fun/dot_product.hpp>
#include <vector>
#include <type_traits>

namespace stan {
namespace math {

template <typename T1, int R1, int C1, typename T2, int R2, int C2>
inline typename std::enable_if<std::is_same<T1, var>::value
                                   || std::is_same<T2, var>::value,
                               Eigen::Matrix<var, 1, C1> >::type
columns_dot_product(const Eigen::Matrix<T1, R1, C1>& v1,
                    const Eigen::Matrix<T2, R2, C2>& v2) {
  check_matching_sizes("dot_product", "v1", v1, "v2", v2);
  Eigen::Matrix<var, 1, C1> ret(1, v1.cols());
  for (size_type j = 0; j < v1.cols(); ++j) {
    ret(j) = var(new internal::dot_product_vari<T1, T2>(v1.col(j), v2.col(j)));
  }
  return ret;
}

}  // namespace math
}  // namespace stan
#endif
