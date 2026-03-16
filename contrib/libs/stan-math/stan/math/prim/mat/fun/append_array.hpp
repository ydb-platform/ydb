#ifndef STAN_MATH_PRIM_MAT_FUN_APPEND_ARRAY_HPP
#define STAN_MATH_PRIM_MAT_FUN_APPEND_ARRAY_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/dims.hpp>
#include <stan/math/prim/mat/fun/resize.hpp>
#include <stan/math/prim/mat/fun/assign.hpp>
#include <stan/math/prim/mat/meta/append_return_type.hpp>
#include <stan/math/prim/arr/err/check_matching_sizes.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <vector>

namespace stan {
namespace math {
/**
 * Return the concatenation of two specified vectors in the order of
 *   the arguments.
 *
 * The return type is computed with append_return_type
 *
 * @tparam T1 Scalar type of first vector
 * @tparam T2 Scalar type of second vector
 * @param x First vector
 * @param y Second vector
 * @return A vector of x and y concatenated together (in that order)
 */
template <typename T1, typename T2>
inline typename append_return_type<std::vector<T1>, std::vector<T2> >::type
append_array(const std::vector<T1>& x, const std::vector<T2>& y) {
  typename append_return_type<std::vector<T1>, std::vector<T2> >::type z;
  std::vector<int> zdims;
  if (x.empty()) {
    zdims = dims(y);
    zdims[0] += x.size();
  } else {
    zdims = dims(x);
    zdims[0] += y.size();
  }
  resize(z, zdims);
  for (size_t i = 0; i < x.size(); ++i)
    assign(z[i], x[i]);
  for (size_t i = 0; i < y.size(); ++i)
    assign(z[i + x.size()], y[i]);
  return z;
}

/**
 * Return the concatenation of two specified vectors in the order of
 *   the arguments.
 *
 * @tparam T1 Type of vectors
 * @param x First vector
 * @param y Second vector
 * @return A vector of x and y concatenated together (in that order)
 */
template <typename T1>
inline std::vector<T1> append_array(const std::vector<T1>& x,
                                    const std::vector<T1>& y) {
  std::vector<T1> z;

  if (!x.empty() && !y.empty()) {
    std::vector<int> xdims = dims(x), ydims = dims(y);
    check_matching_sizes("append_array", "dimension of x", xdims,
                         "dimension of y", ydims);
    for (size_t i = 1; i < xdims.size(); ++i) {
      check_size_match("append_array", "shape of x", xdims[i], "shape of y",
                       ydims[i]);
    }
  }

  z.reserve(x.size() + y.size());
  z.insert(z.end(), x.begin(), x.end());
  z.insert(z.end(), y.begin(), y.end());
  return z;
}
}  // namespace math
}  // namespace stan
#endif
