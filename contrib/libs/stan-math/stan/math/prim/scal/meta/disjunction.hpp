#ifndef STAN_MATH_PRIM_SCAL_META_DISJUNCTION_HPP
#define STAN_MATH_PRIM_SCAL_META_DISJUNCTION_HPP

#include <type_traits>

namespace stan {
namespace math {
/**
 * Extends std::false_type when instantiated with zero or more template
 * parameters, all of which extend the std::false_type. Extends std::true_type
 * if any of them extend the std::true_type.
 */
template <typename... Conds>
struct disjunction : std::false_type {};

template <typename Cond, typename... Conds>
struct disjunction<Cond, Conds...>
    : std::conditional<Cond::value, std::true_type,
                       disjunction<Conds...>>::type {};

}  // namespace math
}  // namespace stan
#endif
