#ifndef STAN_MATH_PRIM_SCAL_META_AD_PROMOTABLE_HPP
#define STAN_MATH_PRIM_SCAL_META_AD_PROMOTABLE_HPP

#include <type_traits>

namespace stan {
namespace math {

/**
 * Template traits metaprogram to determine if a variable of one
 * template type can be promoted to a second target template
 * type. All variables are promotable to themselves, and all
 * primitive arithmetic types are promotable to double.
 *
 * <p>It will declare an enum <code>value</code> equal to
 * <code>false</code>.
 *
 * @tparam V promoted type
 * @tparam T target type
 */
template <typename V, typename T>
struct ad_promotable {
  enum { value = false };
};

/**
 * Any type may be promoted to itself.
 *
 * @tparam T promoted and target type
 */
template <typename T>
struct ad_promotable<
    typename std::enable_if<std::is_arithmetic<T>::value, T>::type, T> {
  enum { value = true };
};

/**
 * A bool may be promoted to a double.
 */
template <>
struct ad_promotable<bool, double> {
  enum { value = true };
};

/**
 * A char may be promoted to a double.
 */
template <>
struct ad_promotable<char, double> {
  enum { value = true };
};

/**
 * An unsigned char may be promoted to a double.
 */
template <>
struct ad_promotable<unsigned char, double> {
  enum { value = true };
};

/**
 * A short may be promoted to a double.
 */
template <>
struct ad_promotable<short, double> {  // NOLINT(runtime/int)
  enum { value = true };
};

/**
 * An unsigned short may be promoted to a double.
 */
template <>
struct ad_promotable<unsigned short, double> {  // NOLINT(runtime/int)
  enum { value = true };
};

/**
 * An int may be promoted to a double.
 */
template <>
struct ad_promotable<int, double> {
  enum { value = true };
};

/**
 * An unsigned int may be promoted to a double.
 */
template <>
struct ad_promotable<unsigned int, double> {
  enum { value = true };
};

/**
 * A long may be promoted to a double.
 */
template <>
struct ad_promotable<long, double> {  // NOLINT(runtime/int)
  enum { value = true };
};

/**
 * An unsigned long may be promoted to a double.
 */
template <>
struct ad_promotable<unsigned long, double> {  // NOLINT(runtime/int)
  enum { value = true };
};

/**
 * A long long may be promoted to a double.
 */
template <>
struct ad_promotable<long long, double> {  // NOLINT(runtime/int)
  enum { value = true };
};

/**
 * An unsigned long long may be promoted to a double.
 */
template <>
struct ad_promotable<unsigned long long, double> {  // NOLINT(runtime/int)
  enum { value = true };
};

/**
 * A float may be promoted to a double.
 */
template <>
struct ad_promotable<float, double> {
  enum { value = true };
};

/**
 * A double may be promoted to a double.
 */
template <>
struct ad_promotable<double, double> {
  enum { value = true };
};

/**
 * A long double may be promoted to a double.
 */
template <>
struct ad_promotable<long double, double> {
  enum { value = true };
};

}  // namespace math
}  // namespace stan
#endif
