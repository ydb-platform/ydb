
#ifndef STAN_MATH_PRIM_SCAL_META_LENGTH_MVT_HPP
#define STAN_MATH_PRIM_SCAL_META_LENGTH_MVT_HPP

#include <stdexcept>

namespace stan {

/**
 * length_mvt provides the length of a multivariate argument.
 *
 * This is the default template function. For any scalar type, this
 * will throw an std::invalid_argument exception since a scalar is not
 * a multivariate structure.
 *
 * @tparam T type to take length of. The default template function should
 *   only match scalars.
 * @throw std::invalid_argument since the type is a scalar.
 */
template <typename T>
size_t length_mvt(const T& /* unused */) {
  throw std::invalid_argument("length_mvt passed to an unrecognized type.");
  return 1U;
}

}  // namespace stan
#endif
