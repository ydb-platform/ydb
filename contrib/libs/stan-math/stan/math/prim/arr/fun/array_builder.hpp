#ifndef STAN_MATH_PRIM_ARR_FUN_ARRAY_BUILDER_HPP
#define STAN_MATH_PRIM_ARR_FUN_ARRAY_BUILDER_HPP

#include <stan/math/prim/scal/fun/promote_elements.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Structure for building up arrays in an expression (rather than
 * in statements) using an argumentchaining add() method and
 * a getter method array() to return the result.
 * Array elements are held in std::vector of type T.
 *
 * @tparam T type of array elements
 */
template <typename T>
class array_builder {
 private:
  std::vector<T> x_;

 public:
  /**
   * Construct an array_builder.
   */
  array_builder() : x_() {}

  /**
   * Add one element of type S to array, promoting to type T.
   *
   * @param u element to add
   * @returns this array_builder object
   */
  template <typename S>
  array_builder& add(const S& u) {
    x_.push_back(promote_elements<T, S>::promote(u));
    return *this;
  }

  /**
   * Getter method to return array itself.
   *
   * @returns std:vector<T> of composed array elements.
   */
  std::vector<T> array() { return x_; }
};

}  // namespace math
}  // namespace stan
#endif
