#ifndef STAN_MATH_REV_CORE_SET_ZERO_ALL_ADJOINTS_NESTED_HPP
#define STAN_MATH_REV_CORE_SET_ZERO_ALL_ADJOINTS_NESTED_HPP

#include <stan/math/rev/core/vari.hpp>
#include <stan/math/rev/core/chainable_alloc.hpp>
#include <stan/math/rev/core/chainablestack.hpp>
#include <stan/math/rev/core/empty_nested.hpp>
#include <stdexcept>

namespace stan {
namespace math {

/**
 * Reset all adjoint values in the top nested portion of the stack
 * to zero.
 */
static void set_zero_all_adjoints_nested() {
  if (empty_nested())
    throw std::logic_error(
        "empty_nested() must be false before calling"
        " set_zero_all_adjoints_nested()");
  size_t start1 = ChainableStack::instance().nested_var_stack_sizes_.back();
  // avoid wrap with unsigned when start1 == 0
  for (size_t i = (start1 == 0U) ? 0U : (start1 - 1);
       i < ChainableStack::instance().var_stack_.size(); ++i)
    ChainableStack::instance().var_stack_[i]->set_zero_adjoint();

  size_t start2
      = ChainableStack::instance().nested_var_nochain_stack_sizes_.back();
  for (size_t i = (start2 == 0U) ? 0U : (start2 - 1);
       i < ChainableStack::instance().var_nochain_stack_.size(); ++i) {
    ChainableStack::instance().var_nochain_stack_[i]->set_zero_adjoint();
  }
}

}  // namespace math
}  // namespace stan
#endif
