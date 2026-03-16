#ifndef STAN_MATH_PRIM_ARR_FUN_PROMOTE_ELEMENTS_HPP
#define STAN_MATH_PRIM_ARR_FUN_PROMOTE_ELEMENTS_HPP

#include <stan/math/prim/scal/fun/promote_elements.hpp>
#include <vector>
#include <cstddef>

namespace stan {
namespace math {

/**
 * Struct with static function for elementwise type promotion.
 *
 * <p>This specialization promotes vector elements of different types
 * which must be compatible with promotion.
 *
 * @tparam T type of promoted elements
 * @tparam S type of input elements, must be assignable to T
 */
template <typename T, typename S>
struct promote_elements<std::vector<T>, std::vector<S> > {
  /**
   * Return input vector of type S as vector of type T.
   *
   * @param u vector of type S, assignable to type T
   * @returns vector of type T
   */
  inline static std::vector<T> promote(const std::vector<S>& u) {
    std::vector<T> t;
    t.reserve(u.size());
    for (size_t i = 0; i < u.size(); ++i)
      t.push_back(promote_elements<T, S>::promote(u[i]));
    return t;
  }
};

/**
 * Struct with static function for elementwise type promotion.
 *
 * <p>This specialization promotes vector elements of the same type.
 *
 * @tparam T type of elements
 */
template <typename T>
struct promote_elements<std::vector<T>, std::vector<T> > {
  /**
   * Return input vector.
   *
   * @param u vector of type T
   * @returns vector of type T
   */
  inline static const std::vector<T>& promote(const std::vector<T>& u) {
    return u;
  }
};

}  // namespace math
}  // namespace stan

#endif
