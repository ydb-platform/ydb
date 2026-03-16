#ifndef STAN_MATH_PRIM_SCAL_ERR_CHECK_SIZE_MATCH_HPP
#define STAN_MATH_PRIM_SCAL_ERR_CHECK_SIZE_MATCH_HPP

#include <boost/type_traits/common_type.hpp>
#include <stan/math/prim/scal/err/invalid_argument.hpp>
#include <sstream>
#include <string>

namespace stan {
namespace math {

/**
 * Check if the provided sizes match.
 * @tparam T_size1 Type of size 1
 * @tparam T_size2 Type of size 2
 * @param function Function name (for error messages)
 * @param name_i Variable name 1 (for error messages)
 * @param i Variable size 1
 * @param name_j Variable name 2 (for error messages)
 * @param j Variable size 2
 * @throw <code>std::invalid_argument</code> if the sizes do not match
 */
template <typename T_size1, typename T_size2>
inline void check_size_match(const char* function, const char* name_i,
                             T_size1 i, const char* name_j, T_size2 j) {
  if (i == static_cast<T_size1>(j))
    return;

  std::ostringstream msg;
  msg << ") and " << name_j << " (" << j << ") must match in size";
  std::string msg_str(msg.str());
  invalid_argument(function, name_i, i, "(", msg_str.c_str());
}

/**
 * Check if the provided sizes match.
 * @tparam T_size1 Type of size 1
 * @tparam T_size2 Type of size 2
 * @param function Function name (for error messages)
 * @param expr_i Expression for variable name 1 (for error messages)
 * @param name_i Variable name 1 (for error messages)
 * @param i Variable size 1
 * @param expr_j Expression for variable name 2 (for error messages)
 * @param name_j Variable name 2 (for error messages)
 * @param j Variable size 2
 * @throw <code>std::invalid_argument</code> if the sizes do not match
 */
template <typename T_size1, typename T_size2>
inline void check_size_match(const char* function, const char* expr_i,
                             const char* name_i, T_size1 i, const char* expr_j,
                             const char* name_j, T_size2 j) {
  if (i == static_cast<T_size1>(j))
    return;
  std::ostringstream updated_name;
  updated_name << expr_i << name_i;
  std::string updated_name_str(updated_name.str());
  std::ostringstream msg;
  msg << ") and " << expr_j << name_j << " (" << j << ") must match in size";
  std::string msg_str(msg.str());
  invalid_argument(function, updated_name_str.c_str(), i, "(", msg_str.c_str());
}

}  // namespace math
}  // namespace stan
#endif
