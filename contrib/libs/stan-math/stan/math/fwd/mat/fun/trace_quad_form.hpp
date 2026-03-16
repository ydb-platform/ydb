#ifndef STAN_MATH_FWD_MAT_FUN_TRACE_QUAD_FORM_HPP
#define STAN_MATH_FWD_MAT_FUN_TRACE_QUAD_FORM_HPP

#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/fwd/mat/fun/multiply.hpp>
#include <stan/math/prim/mat/fun/multiply.hpp>
#include <stan/math/prim/mat/fun/transpose.hpp>
#include <stan/math/prim/mat/fun/trace.hpp>
#include <stan/math/fwd/core.hpp>

namespace stan {
namespace math {

template <int RA, int CA, int RB, int CB, typename T>
inline fvar<T> trace_quad_form(const Eigen::Matrix<fvar<T>, RA, CA> &A,
                               const Eigen::Matrix<fvar<T>, RB, CB> &B) {
  check_square("trace_quad_form", "A", A);
  check_multiplicable("trace_quad_form", "A", A, "B", B);
  return trace(multiply(transpose(B), multiply(A, B)));
}

template <int RA, int CA, int RB, int CB, typename T>
inline fvar<T> trace_quad_form(const Eigen::Matrix<fvar<T>, RA, CA> &A,
                               const Eigen::Matrix<double, RB, CB> &B) {
  check_square("trace_quad_form", "A", A);
  check_multiplicable("trace_quad_form", "A", A, "B", B);
  return trace(multiply(transpose(B), multiply(A, B)));
}

template <int RA, int CA, int RB, int CB, typename T>
inline fvar<T> trace_quad_form(const Eigen::Matrix<double, RA, CA> &A,
                               const Eigen::Matrix<fvar<T>, RB, CB> &B) {
  check_square("trace_quad_form", "A", A);
  check_multiplicable("trace_quad_form", "A", A, "B", B);
  return trace(multiply(transpose(B), multiply(A, B)));
}
}  // namespace math
}  // namespace stan

#endif
