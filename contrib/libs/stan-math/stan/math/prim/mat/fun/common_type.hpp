#ifndef STAN_MATH_PRIM_MAT_FUN_COMMON_TYPE_HPP
#define STAN_MATH_PRIM_MAT_FUN_COMMON_TYPE_HPP

#include <stan/math/prim/arr/fun/common_type.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {
/**
 * Struct which calculates type promotion over two types.
 *
 * <p>This specialization is for matrix types.
 *
 * @tparam T1 type of elements contained in Eigen::Matrix<T1>
 * @tparam T2 type of elements contained in Eigen::Matrix<T2>
 * @tparam R number of rows
 * @tparam C number of columns
 */
template <typename T1, typename T2, int R, int C>
struct common_type<Eigen::Matrix<T1, R, C>, Eigen::Matrix<T2, R, C> > {
  typedef Eigen::Matrix<typename common_type<T1, T2>::type, R, C> type;
};

}  // namespace math
}  // namespace stan

#endif
