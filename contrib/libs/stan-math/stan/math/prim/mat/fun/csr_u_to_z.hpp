#ifndef STAN_MATH_PRIM_MAT_FUN_CSR_U_TO_Z
#define STAN_MATH_PRIM_MAT_FUN_CSR_U_TO_Z

#include <stan/math/prim/mat/err/check_range.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stdexcept>
#include <vector>

namespace stan {
namespace math {

/** \addtogroup csr_format
 *  @{
 */

/**
 * Return the z vector computed from the specified u vector at the
 * index for the z vector.
 *
 * @param[in] u U vector.
 * @param[in] i Index into resulting z vector.
 * @return z[i] where z is conversion from u.
 * @throw std::domain_error if u is zero length.
 * @throw std::out_of_range if i is out of range.
 */
inline int csr_u_to_z(const std::vector<int>& u, int i) {
  check_positive("csr_u_to_z", "u.size()", u.size());
  check_range("csr_u_to_z", "i", u.size(), i + 1, "index out of range");
  return u[i + 1] - u[i];
}

}  // namespace math
}  // namespace stan
#endif
