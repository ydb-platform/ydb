#ifndef STAN_MATH_REV_MAT_FUN_QUAD_FORM_SYM_HPP
#define STAN_MATH_REV_MAT_FUN_QUAD_FORM_SYM_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/rev/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/fun/value_of.hpp>
#include <stan/math/prim/mat/fun/quad_form.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/prim/mat/err/check_symmetric.hpp>
#include <stan/math/rev/mat/fun/quad_form.hpp>
#include <type_traits>

namespace stan {
namespace math {

template <typename Ta, int Ra, int Ca, typename Tb, int Rb, int Cb>
inline typename std::enable_if<std::is_same<Ta, var>::value
                                   || std::is_same<Tb, var>::value,
                               Eigen::Matrix<var, Cb, Cb> >::type
quad_form_sym(const Eigen::Matrix<Ta, Ra, Ca>& A,
              const Eigen::Matrix<Tb, Rb, Cb>& B) {
  check_square("quad_form", "A", A);
  check_symmetric("quad_form_sym", "A", A);
  check_multiplicable("quad_form_sym", "A", A, "B", B);

  internal::quad_form_vari<Ta, Ra, Ca, Tb, Rb, Cb>* baseVari
      = new internal::quad_form_vari<Ta, Ra, Ca, Tb, Rb, Cb>(A, B, true);

  return baseVari->impl_->C_;
}

template <typename Ta, int Ra, int Ca, typename Tb, int Rb>
inline typename std::enable_if<
    std::is_same<Ta, var>::value || std::is_same<Tb, var>::value, var>::type
quad_form_sym(const Eigen::Matrix<Ta, Ra, Ca>& A,
              const Eigen::Matrix<Tb, Rb, 1>& B) {
  check_square("quad_form", "A", A);
  check_symmetric("quad_form_sym", "A", A);
  check_multiplicable("quad_form_sym", "A", A, "B", B);

  internal::quad_form_vari<Ta, Ra, Ca, Tb, Rb, 1>* baseVari
      = new internal::quad_form_vari<Ta, Ra, Ca, Tb, Rb, 1>(A, B, true);

  return baseVari->impl_->C_(0, 0);
}

}  // namespace math
}  // namespace stan
#endif
