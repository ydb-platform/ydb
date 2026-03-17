#ifndef STAN_MATH_REV_ARR_FUN_TO_VAR_HPP
#define STAN_MATH_REV_ARR_FUN_TO_VAR_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/rev/scal/fun/to_var.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Converts argument to an automatic differentiation variable.
 *
 * Returns a var variable with the input value.
 *
 * @param[in] v A std::vector<double>
 * @return A std::vector<var> with the values set
 */
inline std::vector<var> to_var(const std::vector<double>& v) {
  std::vector<var> var_vector(v.size());
  for (size_t n = 0; n < v.size(); n++)
    var_vector[n] = v[n];
  return var_vector;
}

/**
 * Converts argument to an automatic differentiation variable.
 *
 * Returns a var variable with the input value.
 *
 * @param[in] v A std::vector<var>
 * @return A std::vector<var>
 */
inline std::vector<var> to_var(const std::vector<var>& v) { return v; }

}  // namespace math
}  // namespace stan
#endif
