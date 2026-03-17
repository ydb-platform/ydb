#ifndef STAN_MATH_PRIM_SCAL_META_IS_CONSTANT_HPP
#define STAN_MATH_PRIM_SCAL_META_IS_CONSTANT_HPP

#include <type_traits>

namespace stan {

/**
 * Metaprogramming struct to detect whether a given type is constant
 * in the mathematical sense (not the C++ <code>const</code>
 * sense). If the parameter type is constant, <code>value</code>
 * will be equal to <code>true</code>.
 *
 * The baseline implementation in this abstract base class is to
 * classify a type <code>T</code> as constant if it can be converted
 * (i.e., assigned) to a <code>double</code>.  This baseline should
 * be overridden for any type that should be treated as a variable.
 *
 * @tparam T Type being tested.
 */
template <typename T>
struct is_constant {
  /**
   * A boolean constant with equal to <code>true</code> if the
   * type parameter <code>T</code> is a mathematical constant.
   */
  enum { value = std::is_convertible<T, double>::value };
};

}  // namespace stan
#endif
