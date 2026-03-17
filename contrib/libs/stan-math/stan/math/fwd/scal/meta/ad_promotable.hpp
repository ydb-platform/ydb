#ifndef STAN_MATH_FWD_SCAL_META_AD_PROMOTABLE_HPP
#define STAN_MATH_FWD_SCAL_META_AD_PROMOTABLE_HPP

#include <stan/math/prim/scal/meta/ad_promotable.hpp>

namespace stan {
namespace math {

template <typename T>
struct fvar;
/**
 * Template traits metaprogram to determine if a variable
 * of one template type is promotable to the base type of
 * a second fvar template type.
 *
 * <p>It will declare an enum <code>value</code> equal to
 * <code>true</code> if the variable type is promotable to
 * the base type of the fvar template type,
 * <code>false</code> otherwise.
 *
 * @tparam T promoted type
 */
template <typename V, typename T>
struct ad_promotable<V, fvar<T> > {
  enum { value = ad_promotable<V, T>::value };
};

}  // namespace math
}  // namespace stan
#endif
