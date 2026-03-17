#ifndef STAN_MATH_REV_CORE_NESTED_SIZE_HPP
#define STAN_MATH_REV_CORE_NESTED_SIZE_HPP

#include <stan/math/rev/core/chainablestack.hpp>
#include <cstdlib>

namespace stan {
namespace math {

static inline size_t nested_size() {
  return ChainableStack::instance().var_stack_.size()
         - ChainableStack::instance().nested_var_stack_sizes_.back();
}

}  // namespace math
}  // namespace stan
#endif
