#ifndef STAN_MATH_REV_MAT_FUN_INITIALIZE_VARIABLE_HPP
#define STAN_MATH_REV_MAT_FUN_INITIALIZE_VARIABLE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/rev/core.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Initialize variable to value.  (Function may look pointless, but
 * its needed to bottom out recursion.)
 */
inline void initialize_variable(var& variable, const var& value) {
  variable = value;
}

/**
 * Initialize every cell in the matrix to the specified value.
 *
 */
template <int R, int C>
inline void initialize_variable(Eigen::Matrix<var, R, C>& matrix,
                                const var& value) {
  for (int i = 0; i < matrix.size(); ++i)
    matrix(i) = value;
}

/**
 * Initialize the variables in the standard vector recursively.
 */
template <typename T>
inline void initialize_variable(std::vector<T>& variables, const var& value) {
  for (size_t i = 0; i < variables.size(); ++i)
    initialize_variable(variables[i], value);
}

}  // namespace math
}  // namespace stan

#endif
