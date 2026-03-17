#ifndef STAN_MATH_REV_CORE_CHAINABLE_ALLOC_HPP
#define STAN_MATH_REV_CORE_CHAINABLE_ALLOC_HPP

#include <stan/math/rev/core/chainablestack.hpp>

namespace stan {
namespace math {

/**
 * A chainable_alloc is an object which is constructed and
 * destructed normally but the memory lifespan is managed along
 * with the arena allocator for the gradient calculation.  A
 * chainable_alloc instance must be created with a call to
 * operator new for memory management.
 */
class chainable_alloc {
 public:
  chainable_alloc() {
    ChainableStack::instance().var_alloc_stack_.push_back(this);
  }
  virtual ~chainable_alloc() {}
};

}  // namespace math
}  // namespace stan
#endif
