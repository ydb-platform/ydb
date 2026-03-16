#ifndef STAN_MATH_PRIM_SCAL_META_IS_FVAR_HPP
#define STAN_MATH_PRIM_SCAL_META_IS_FVAR_HPP

namespace stan {
/**
 * Defines a public enum named value which is defined to be false
 * as the primitive scalar types cannot be a stan::math::fvar type.
 */
template <typename T>
struct is_fvar {
  enum { value = false };
};

}  // namespace stan
#endif
