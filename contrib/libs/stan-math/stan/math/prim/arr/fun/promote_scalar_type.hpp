#ifndef STAN_MATH_PRIM_ARR_FUN_PROMOTE_SCALAR_TYPE_HPP
#define STAN_MATH_PRIM_ARR_FUN_PROMOTE_SCALAR_TYPE_HPP

#include <stan/math/prim/scal/fun/promote_scalar_type.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Template metaprogram to calculate a type for a container whose
 * underlying scalar is converted from the second template
 * parameter type to the first.
 *
 * @tparam T result scalar type.
 * @tparam S input type
 */
template <typename T, typename S>
struct promote_scalar_type<T, std::vector<S> > {
  /**
   * The promoted type.
   */
  typedef std::vector<typename promote_scalar_type<T, S>::type> type;
};

}  // namespace math
}  // namespace stan
#endif
