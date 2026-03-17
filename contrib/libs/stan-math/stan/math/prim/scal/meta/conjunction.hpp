#ifndef STAN_MATH_PRIM_SCAL_META_CONJUNCTION_HPP
#define STAN_MATH_PRIM_SCAL_META_CONJUNCTION_HPP

#include <type_traits>

namespace stan {
namespace math {
/**
 * Extends std::true_type when instantiated with zero or more template
 * parameters, all of which extend the std::true_type. Extends std::false_type
 * if any of them extend the std::false_type.
 */
template <typename... T>
struct conjunction : std::true_type {};

template <typename T, typename... Ts>
struct conjunction<T, Ts...>
    : std::conditional<T::value, conjunction<Ts...>, std::false_type>::type {};

}  // namespace math
}  // namespace stan
#endif
