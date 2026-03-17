#ifndef STAN_MATH_REV_CORE_SET_ZERO_ALL_ADJOINTS_HPP
#define STAN_MATH_REV_CORE_SET_ZERO_ALL_ADJOINTS_HPP

#include <stan/math/rev/core/vari.hpp>
#include <stan/math/rev/core/chainable_alloc.hpp>
#include <stan/math/rev/core/chainablestack.hpp>

namespace stan {
namespace math {

/**
 * Reset all adjoint values in the stack to zero.
 */
static void set_zero_all_adjoints() {
  for (auto &x : ChainableStack::instance().var_stack_)
    x->set_zero_adjoint();
  for (auto &x : ChainableStack::instance().var_nochain_stack_)
    x->set_zero_adjoint();
}

}  // namespace math
}  // namespace stan
#endif
