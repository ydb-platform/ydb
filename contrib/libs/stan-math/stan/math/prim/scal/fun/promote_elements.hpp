#ifndef STAN_MATH_PRIM_SCAL_FUN_PROMOTE_ELEMENTS_HPP
#define STAN_MATH_PRIM_SCAL_FUN_PROMOTE_ELEMENTS_HPP

namespace stan {
namespace math {

/**
 * Struct with static function for elementwise type promotion.
 *
 * <p>This base implementation promotes one scalar value to another.
 *
 * @tparam T type of promoted element
 * @tparam S type of input element, must be assignable to T
 */
template <typename T, typename S>
struct promote_elements {
  /**
   * Return input element.
   *
   * @param u input of type S, assignable to type T
   * @returns input as type T
   */
  inline static T promote(const S& u) { return u; }
};

/**
 * Struct with static function for elementwise type promotion.
 *
 * <p>This specialization promotes scalar values of the same type.
 *
 * @tparam T type of elements
 */
template <typename T>
struct promote_elements<T, T> {
  /**
   * Return input element.
   *
   * @param u input of type T
   * @returns input as type T
   */
  inline static const T& promote(const T& u) { return u; }
};

}  // namespace math
}  // namespace stan

#endif
