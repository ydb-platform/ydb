#ifndef STAN_MATH_PRIM_ARR_FUN_PROMOTE_SCALAR_HPP
#define STAN_MATH_PRIM_ARR_FUN_PROMOTE_SCALAR_HPP

#include <stan/math/prim/scal/fun/promote_scalar.hpp>
#include <stan/math/prim/scal/fun/promote_scalar_type.hpp>
#include <stan/math/prim/arr/meta/index_type.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Struct to hold static function for promoting underlying scalar
 * types.  This specialization is for standard vector inputs.
 *
 * @tparam T return scalar type
 * @tparam S input type for standard vector elements in static
 * nested function, which must have an underlying scalar type
 * assignable to T.
 */
template <typename T, typename S>
struct promote_scalar_struct<T, std::vector<S> > {
  /**
   * Return the standard vector consisting of the recursive
   * promotion of the elements of the input standard vector to the
   * scalar type specified by the return template parameter.
   *
   * @param x input standard vector.
   * @return standard vector with values promoted from input vector.
   */
  static std::vector<typename promote_scalar_type<T, S>::type> apply(
      const std::vector<S>& x) {
    typedef std::vector<typename promote_scalar_type<T, S>::type> return_t;
    typedef typename index_type<return_t>::type idx_t;
    return_t y(x.size());
    for (idx_t i = 0; i < x.size(); ++i)
      y[i] = promote_scalar_struct<T, S>::apply(x[i]);
    return y;
  }
};

}  // namespace math
}  // namespace stan
#endif
