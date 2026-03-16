#ifndef STAN_MATH_PRIM_MAT_FUN_GET_LP_HPP
#define STAN_MATH_PRIM_MAT_FUN_GET_LP_HPP

#include <boost/math/tools/promotion.hpp>
#include <stan/math/prim/mat/fun/accumulator.hpp>

namespace stan {
namespace math {

template <typename T_lp, typename T_lp_accum>
inline typename boost::math::tools::promote_args<T_lp, T_lp_accum>::type get_lp(
    const T_lp& lp, const accumulator<T_lp_accum>& lp_accum) {
  return lp + lp_accum.sum();
}

}  // namespace math
}  // namespace stan
#endif
