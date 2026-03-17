#ifndef STAN_MATH_PRIM_SCAL_META_LENGTH_HPP
#define STAN_MATH_PRIM_SCAL_META_LENGTH_HPP

#include <cstdlib>

namespace stan {
/**
 * Returns the length of primitive scalar types
 * that are always of length 1.
 */
template <typename T>
size_t length(const T& /*x*/) {
  return 1U;
}
}  // namespace stan
#endif
