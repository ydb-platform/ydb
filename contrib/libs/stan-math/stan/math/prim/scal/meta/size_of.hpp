#ifndef STAN_MATH_PRIM_SCAL_META_SIZE_OF_HPP
#define STAN_MATH_PRIM_SCAL_META_SIZE_OF_HPP

#include <stan/math/prim/scal/meta/is_vector.hpp>
#include <cstddef>

namespace stan {

template <typename T, bool is_vec>
struct size_of_helper {
  static size_t size_of(const T& /*x*/) { return 1U; }
};

template <typename T>
struct size_of_helper<T, true> {
  static size_t size_of(const T& x) { return x.size(); }
};

/**
 * Returns the size of the provided vector or
 * the constant 1 if the input argument is not a vector.
 * @param x value for which to obtain the size of
 * @tparam the type of the input value
 * @return the size of x or 1 if not a vector
 */
template <typename T>
size_t size_of(const T& x) {
  return size_of_helper<T, is_vector<T>::value>::size_of(x);
}

}  // namespace stan
#endif
