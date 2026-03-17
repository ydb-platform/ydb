#ifndef STAN_MATH_FWD_SCAL_META_OPERANDS_AND_PARTIALS_HPP
#define STAN_MATH_FWD_SCAL_META_OPERANDS_AND_PARTIALS_HPP

#include <stan/math/prim/scal/meta/broadcast_array.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/fwd/core/fvar.hpp>

namespace stan {
namespace math {
namespace internal {
template <typename Dx>
class ops_partials_edge<Dx, fvar<Dx> > {
 public:
  typedef fvar<Dx> Op;
  Dx partial_;
  broadcast_array<Dx> partials_;
  explicit ops_partials_edge(const Op& op)
      : partial_(0), partials_(partial_), operand_(op) {}

 private:
  template <typename, typename, typename, typename, typename, typename>
  friend class stan::math::operands_and_partials;
  const Op& operand_;

  Dx dx() { return this->partials_[0] * this->operand_.d_; }
};
}  // namespace internal

/**
 * This class builds partial derivatives with respect to a set of
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
 * This class now supports multivariate use-cases as well by
 * exposing edge#_.partials_vec
 *
 * This is the specialization for when the return type is fvar,
 * which should be for forward mode and all higher-order cases.
 *
 * NB: since ops_partials_edge.partials_ and ops_partials_edge.partials_vec
 * are sometimes represented internally as a broadcast_array, we need to take
 * care with assignments to them. Indeed, we can assign any right hand side
 * which allows for indexing to a broadcast_array. The resulting behaviour is
 * that the entry for the first index is what gets assigned. The most common
 * use-case should be where the rhs is some container of length 1.
 *
 * @tparam Op1 type of the first operand
 * @tparam Op2 type of the second operand
 * @tparam Op3 type of the third operand
 * @tparam Op4 type of the fourth operand
 * @tparam Op5 type of the fifth operand
 * @tparam T_return_type return type of the expression. This defaults
 *   to a template metaprogram that calculates the scalar promotion of
 *   Op1 -- Op5
 */
template <typename Op1, typename Op2, typename Op3, typename Op4, typename Op5,
          typename Dx>
class operands_and_partials<Op1, Op2, Op3, Op4, Op5, fvar<Dx> > {
 public:
  internal::ops_partials_edge<Dx, Op1> edge1_;
  internal::ops_partials_edge<Dx, Op2> edge2_;
  internal::ops_partials_edge<Dx, Op3> edge3_;
  internal::ops_partials_edge<Dx, Op4> edge4_;
  internal::ops_partials_edge<Dx, Op5> edge5_;
  typedef fvar<Dx> T_return_type;
  explicit operands_and_partials(const Op1& o1) : edge1_(o1) {}
  operands_and_partials(const Op1& o1, const Op2& o2)
      : edge1_(o1), edge2_(o2) {}
  operands_and_partials(const Op1& o1, const Op2& o2, const Op3& o3)
      : edge1_(o1), edge2_(o2), edge3_(o3) {}
  operands_and_partials(const Op1& o1, const Op2& o2, const Op3& o3,
                        const Op4& o4)
      : edge1_(o1), edge2_(o2), edge3_(o3), edge4_(o4) {}
  operands_and_partials(const Op1& o1, const Op2& o2, const Op3& o3,
                        const Op4& o4, const Op5& o5)
      : edge1_(o1), edge2_(o2), edge3_(o3), edge4_(o4), edge5_(o5) {}

  /**
   * Build the node to be stored on the autodiff graph.
   * This should contain both the value and the tangent.
   *
   * For scalars, we don't calculate any tangents.
   * For reverse mode, we end up returning a type of var that will calculate
   * the appropriate adjoint using the stored operands and partials.
   * Forward mode just calculates the tangent on the spot and returns it in
   * a vanilla fvar.
   *
   * @param value the return value of the function we are compressing
   * @return the value with its derivative
   */
  T_return_type build(Dx value) {
    Dx deriv
        = edge1_.dx() + edge2_.dx() + edge3_.dx() + edge4_.dx() + edge5_.dx();
    return T_return_type(value, deriv);
  }
};
}  // namespace math
}  // namespace stan
#endif
