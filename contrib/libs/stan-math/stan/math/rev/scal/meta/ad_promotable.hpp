#ifndef STAN_MATH_REV_SCAL_META_AD_PROMOTABLE_HPP
#define STAN_MATH_REV_SCAL_META_AD_PROMOTABLE_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/meta/ad_promotable.hpp>

namespace stan {
namespace math {
/**
 * Template traits metaprogram to determine if a variable of one
 * template type is promotable to var.
 *
 * <p>It will declare an enum <code>value</code> equal to
 * <code>true</code> if the type is promotable to double,
 * <code>false</code> otherwise.
 *
 * @tparam T promoted type
 */
template <typename T>
struct ad_promotable<T, var> {
  enum { value = ad_promotable<T, double>::value };
};
/**
 * A var type is promotable to itself.
 */
template <>
struct ad_promotable<var, var> {
  enum { value = true };
};

}  // namespace math
}  // namespace stan
#endif
