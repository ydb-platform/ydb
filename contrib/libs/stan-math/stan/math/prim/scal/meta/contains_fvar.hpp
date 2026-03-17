#ifndef STAN_MATH_PRIM_SCAL_META_CONTAINS_FVAR_HPP
#define STAN_MATH_PRIM_SCAL_META_CONTAINS_FVAR_HPP

#include <stan/math/prim/scal/meta/is_fvar.hpp>
#include <stan/math/prim/scal/meta/scalar_type.hpp>
#include <stan/math/prim/scal/meta/disjunction.hpp>

namespace stan {

/**
 * Extends std::true_type when instantiated with at least 1
 * template parameter that is a fvar. Extends std::false_type
 * otherwise.
 * @tparam T Types to test
 */
template <typename... T>
using contains_fvar
    = math::disjunction<is_fvar<typename scalar_type<T>::type>...>;

}  // namespace stan
#endif
