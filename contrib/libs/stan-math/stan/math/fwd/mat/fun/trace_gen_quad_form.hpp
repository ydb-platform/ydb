#ifndef STAN_MATH_FWD_MAT_FUN_TRACE_GEN_QUAD_FORM_HPP
#define STAN_MATH_FWD_MAT_FUN_TRACE_GEN_QUAD_FORM_HPP

#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/fwd/mat/fun/multiply.hpp>
#include <stan/math/prim/mat/fun/trace.hpp>
#include <stan/math/prim/mat/fun/multiply.hpp>
#include <stan/math/prim/mat/fun/transpose.hpp>

namespace stan {
namespace math {

template <int RD, int CD, int RA, int CA, int RB, int CB, typename T>
inline fvar<T> trace_gen_quad_form(const Eigen::Matrix<fvar<T>, RD, CD> &D,
                                   const Eigen::Matrix<fvar<T>, RA, CA> &A,
                                   const Eigen::Matrix<fvar<T>, RB, CB> &B) {
  check_square("trace_gen_quad_form", "A", A);
  check_square("trace_gen_quad_form", "D", D);
  check_multiplicable("trace_gen_quad_form", "A", A, "B", B);
  check_multiplicable("trace_gen_quad_form", "B", B, "D", D);
  return trace(multiply(multiply(D, transpose(B)), multiply(A, B)));
}

}  // namespace math
}  // namespace stan
#endif
