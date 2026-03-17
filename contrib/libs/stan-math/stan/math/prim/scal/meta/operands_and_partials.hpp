#ifndef STAN_MATH_PRIM_SCAL_META_OPERANDS_AND_PARTIALS_HPP
#define STAN_MATH_PRIM_SCAL_META_OPERANDS_AND_PARTIALS_HPP

#include <stan/math/prim/scal/meta/broadcast_array.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>

namespace stan {
namespace math {
template <typename Op1 = double, typename Op2 = double, typename Op3 = double,
          typename Op4 = double, typename Op5 = double,
          typename T_return_type =
              typename return_type<Op1, Op2, Op3, Op4, Op5>::type>
class operands_and_partials;  // Forward declaration

namespace internal {
/**
 * An edge holds both the operands and its associated
 * partial derivatives. They're held together in the
 * same class because then we can keep the templating logic that
 * specializes on type of operand in one place.
 *
 * This is the base template class that ends up getting instantiated
 * for arithmetic primitives (doubles and ints).
 *
 * NB: since ops_partials_edge.partials_ and ops_partials_edge.partials_vec
 * are sometimes represented internally as a broadcast_array, we need to take
 * care with assignments to them. Indeed, we can assign any right hand side
 * which allows for indexing to a broadcast_array. The resulting behaviour is
 * that the entry for the first index is what gets assigned. The most common
 * use-case should be where the rhs is some container of length 1.
 *
 * @tparam ViewElt the type we expect to be at partials_[i]
 * @tparam Op the type of the operand
 */
template <typename ViewElt, typename Op>
class ops_partials_edge {
 public:
  empty_broadcast_array<ViewElt, Op> partials_;

  ops_partials_edge() {}
  explicit ops_partials_edge(const Op& /* op */) {}

 private:
  template <typename, typename, typename, typename, typename, typename>
  friend class stan::math::operands_and_partials;

  void dump_partials(ViewElt* /* partials */) const {}  // reverse mode
  void dump_operands(void* /* operands */) const {}     // reverse mode
  ViewElt dx() const { return 0; }                      // used for fvars
  int size() const { return 0; }                        // reverse mode
};
}  // namespace internal

/**
 * This template builds partial derivatives with respect to a
 * set of
 * operands. There are two reason for the generality of this
 * class. The first is to handle vector and scalar arguments
 * without needing to write additional code. The second is to use
 * this class for writing probability distributions that handle
 * primitives, reverse mode, and forward mode variables
 * seamlessly.
 *
 * Conceptually, this class is used when we want to manually calculate
 * the derivative of a function and store this manual result on the
 * autodiff stack in a sort of "compressed" form. Think of it like an
 * easy-to-use interface to rev/core/precomputed_gradients.
 *
 * This class supports nested container ("multivariate") use-cases
 * as well by exposing a partials_vec_ member on edges of the
 * appropriate type.
 *
 * This base template is instantiated when all operands are
 * primitives and we don't want to calculate derivatives at
 * all. So all Op1 - Op5 must be arithmetic primitives
 * like int or double. This is controlled with the
 * T_return_type type parameter.
 *
 * @tparam Op1 type of the first operand
 * @tparam Op2 type of the second operand
 * @tparam Op3 type of the third operand
 * @tparam Op4 type of the fourth operand
 * @tparam Op5 type of the fifth operand
 * @tparam T_return_type return type of the expression. This defaults
 *   to calling a template metaprogram that calculates the scalar
 *   promotion of Op1..Op4
 */
template <typename Op1, typename Op2, typename Op3, typename Op4, typename Op5,
          typename T_return_type>
class operands_and_partials {
 public:
  explicit operands_and_partials(const Op1& /* op1 */) {}
  operands_and_partials(const Op1& /* op1 */, const Op2& /* op2 */) {}
  operands_and_partials(const Op1& /* op1 */, const Op2& /* op2 */,
                        const Op3& /* op3 */) {}
  operands_and_partials(const Op1& /* op1 */, const Op2& /* op2 */,
                        const Op3& /* op3 */, const Op4& /* op4 */) {}
  operands_and_partials(const Op1& /* op1 */, const Op2& /* op2 */,
                        const Op3& /* op3 */, const Op4& /* op4 */,
                        const Op5& /* op5 */) {}

  /**
   * Build the node to be stored on the autodiff graph.
   * This should contain both the value and the tangent.
   *
   * For scalars (this implementation), we don't calculate any derivatives.
   * For reverse mode, we end up returning a type of var that will calculate
   * the appropriate adjoint using the stored operands and partials.
   * Forward mode just calculates the tangent on the spot and returns it in
   * a vanilla fvar.
   *
   * @param value the return value of the function we are compressing
   * @return the value with its derivative
   */
  T_return_type build(double value) { return value; }

  // These will always be 0 size base template instantiations (above).
  internal::ops_partials_edge<double, Op1> edge1_;
  internal::ops_partials_edge<double, Op2> edge2_;
  internal::ops_partials_edge<double, Op3> edge3_;
  internal::ops_partials_edge<double, Op4> edge4_;
  internal::ops_partials_edge<double, Op5> edge5_;
};
}  // namespace math
}  // namespace stan
#endif
