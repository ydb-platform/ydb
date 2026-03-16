#ifndef STAN_MATH_REV_MAT_FUN_TYPEDEFS_HPP
#define STAN_MATH_REV_MAT_FUN_TYPEDEFS_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/rev/core.hpp>

namespace stan {
namespace math {

typedef Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic>::Index size_type;

/**
 * The type of a matrix holding <code>var</code>
 * values.
 */
typedef Eigen::Matrix<var, Eigen::Dynamic, Eigen::Dynamic> matrix_v;

/**
 * The type of a (column) vector holding <code>var</code>
 * values.
 */
typedef Eigen::Matrix<var, Eigen::Dynamic, 1> vector_v;

/**
 * The type of a row vector holding <code>var</code>
 * values.
 */
typedef Eigen::Matrix<var, 1, Eigen::Dynamic> row_vector_v;

}  // namespace math
}  // namespace stan
#endif
