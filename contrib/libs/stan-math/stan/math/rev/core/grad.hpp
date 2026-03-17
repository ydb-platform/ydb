#ifndef STAN_MATH_REV_CORE_GRAD_HPP
#define STAN_MATH_REV_CORE_GRAD_HPP

#include <stan/math/rev/core/chainable_alloc.hpp>
#include <stan/math/rev/core/chainablestack.hpp>
#include <stan/math/rev/core/empty_nested.hpp>
#include <stan/math/rev/core/nested_size.hpp>
#include <stan/math/rev/core/vari.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Compute the gradient for all variables starting from the
 * specified root variable implementation.  Does not recover
 * memory.  This chainable variable's adjoint is initialized using
 * the method <code>init_dependent()</code> and then the chain
 * rule is applied working down the stack from this vari and
 * calling each vari's <code>chain()</code> method in turn.
 *
 * <p>This function computes a nested gradient only going back as far
 * as the last nesting.
 *
 * <p>This function does not recover any memory from the computation.
 *
 * @param vi Variable implementation for root of partial
 * derivative propagation.
 */
static void grad(vari* vi) {
  // simple reference implementation (intended as doc):
  //   vi->init_dependent();
  //   size_t end = var_stack_.size();
  //   size_t begin = empty_nested() ? 0 : end - nested_size();
  //   for (size_t i = end; --i > begin; )
  //     var_stack_[i]->chain();

  typedef std::vector<vari*>::reverse_iterator it_t;
  vi->init_dependent();
  it_t begin = ChainableStack::instance().var_stack_.rbegin();
  it_t end = empty_nested() ? ChainableStack::instance().var_stack_.rend()
                            : begin + nested_size();
  for (it_t it = begin; it < end; ++it) {
    (*it)->chain();
  }
}

}  // namespace math
}  // namespace stan

#endif
