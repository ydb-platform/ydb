#ifndef STAN_MATH_PRIM_SCAL_META_RETURN_TYPE_HPP
#define STAN_MATH_PRIM_SCAL_META_RETURN_TYPE_HPP

#include <stan/math/prim/scal/meta/scalar_type.hpp>
#include <boost/math/tools/promotion.hpp>

namespace stan {

/**
 * Template metaprogram to calculate the base scalar return type resulting
 * from promoting all the scalar types of the template parameters. The
 * metaprogram can take an arbitrary number of template parameters.
 *
 * All C++ primitive types (except <code>long double</code>) are automatically
 * promoted to <code>double</code>.
 *
 * <code>return_type<...></code> is a class defining a single public
 * typedef <code>type</code> that is <code>var</code> if there is a non-constant
 * template argument and is <code>double</code> otherwise. This is consistent
 * with the Stan logic that if code receives <code>var</code> as an input type,
 * then the return type is <code>var</code>. All other functions return
 * <code>double</code>.
 *
 * Example usage:
 *  - <code>return_type<int,double,float>::type</code> is <code>double</code>
 *  - <code>return_type<double,var>::type</code> is <code>var</code>
 *
 * @tparam T (required) A type
 * @tparam Types_pack (optional) A parameter pack containing further types.
 */

template <typename T, typename... Types_pack>
struct return_type {
  typedef typename boost::math::tools::promote_args<
      double, typename scalar_type<T>::type,
      typename return_type<Types_pack...>::type>::type type;
};

template <typename T>
struct return_type<T> {
  typedef typename boost::math::tools::promote_args<
      double, typename scalar_type<T>::type>::type type;
};

}  // namespace stan
#endif
