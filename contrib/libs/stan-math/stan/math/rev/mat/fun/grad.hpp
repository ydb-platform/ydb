#ifndef STAN_MATH_REV_MAT_FUN_GRAD_HPP
#define STAN_MATH_REV_MAT_FUN_GRAD_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/rev/mat/fun/Eigen_NumTraits.hpp>
#include <stan/math/rev/core.hpp>

namespace stan {
namespace math {

/**
 * Propagate chain rule to calculate gradients starting from
 * the specified variable.  Resizes the input vector to be the
 * correct size.
 *
 * The grad() function does not itself recover any memory.  use
 * <code>recover_memory()</code> or
 * <code>recover_memory_nested()</code> to recover memory.
 *
 * @param[in] v Value of function being differentiated
 * @param[in] x Variables being differentiated with respect to
 * @param[out] g Gradient, d/dx v, evaluated at x.
 */
inline void grad(var& v, Eigen::Matrix<var, Eigen::Dynamic, 1>& x,
                 Eigen::VectorXd& g) {
  grad(v.vi_);
  g.resize(x.size());
  for (int i = 0; i < x.size(); ++i)
    g(i) = x(i).vi_->adj_;
}

}  // namespace math
}  // namespace stan
#endif
