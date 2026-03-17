#ifndef STAN_MATH_REV_MAT_FUN_TRACE_QUAD_FORM_HPP
#define STAN_MATH_REV_MAT_FUN_TRACE_QUAD_FORM_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/rev/scal/fun/value_of.hpp>
#include <stan/math/prim/mat/fun/value_of.hpp>
#include <stan/math/rev/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/fun/trace_quad_form.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <type_traits>

namespace stan {
namespace math {
namespace internal {
template <typename Ta, int Ra, int Ca, typename Tb, int Rb, int Cb>
class trace_quad_form_vari_alloc : public chainable_alloc {
 public:
  trace_quad_form_vari_alloc(const Eigen::Matrix<Ta, Ra, Ca>& A,
                             const Eigen::Matrix<Tb, Rb, Cb>& B)
      : A_(A), B_(B) {}

  double compute() { return trace_quad_form(value_of(A_), value_of(B_)); }

  Eigen::Matrix<Ta, Ra, Ca> A_;
  Eigen::Matrix<Tb, Rb, Cb> B_;
};

template <typename Ta, int Ra, int Ca, typename Tb, int Rb, int Cb>
class trace_quad_form_vari : public vari {
 protected:
  static inline void chainA(Eigen::Matrix<double, Ra, Ca>& A,
                            const Eigen::Matrix<double, Rb, Cb>& Bd,
                            double adjC) {}
  static inline void chainB(Eigen::Matrix<double, Rb, Cb>& B,
                            const Eigen::Matrix<double, Ra, Ca>& Ad,
                            const Eigen::Matrix<double, Rb, Cb>& Bd,
                            double adjC) {}

  static inline void chainA(Eigen::Matrix<var, Ra, Ca>& A,
                            const Eigen::Matrix<double, Rb, Cb>& Bd,
                            double adjC) {
    Eigen::Matrix<double, Ra, Ca> adjA(adjC * Bd * Bd.transpose());
    for (int j = 0; j < A.cols(); j++)
      for (int i = 0; i < A.rows(); i++)
        A(i, j).vi_->adj_ += adjA(i, j);
  }
  static inline void chainB(Eigen::Matrix<var, Rb, Cb>& B,
                            const Eigen::Matrix<double, Ra, Ca>& Ad,
                            const Eigen::Matrix<double, Rb, Cb>& Bd,
                            double adjC) {
    Eigen::Matrix<double, Ra, Ca> adjB(adjC * (Ad + Ad.transpose()) * Bd);
    for (int j = 0; j < B.cols(); j++)
      for (int i = 0; i < B.rows(); i++)
        B(i, j).vi_->adj_ += adjB(i, j);
  }

  inline void chainAB(Eigen::Matrix<Ta, Ra, Ca>& A,
                      Eigen::Matrix<Tb, Rb, Cb>& B,
                      const Eigen::Matrix<double, Ra, Ca>& Ad,
                      const Eigen::Matrix<double, Rb, Cb>& Bd, double adjC) {
    chainA(A, Bd, adjC);
    chainB(B, Ad, Bd, adjC);
  }

 public:
  explicit trace_quad_form_vari(
      trace_quad_form_vari_alloc<Ta, Ra, Ca, Tb, Rb, Cb>* impl)
      : vari(impl->compute()), impl_(impl) {}

  virtual void chain() {
    chainAB(impl_->A_, impl_->B_, value_of(impl_->A_), value_of(impl_->B_),
            adj_);
  }

  trace_quad_form_vari_alloc<Ta, Ra, Ca, Tb, Rb, Cb>* impl_;
};
}  // namespace internal

template <typename Ta, int Ra, int Ca, typename Tb, int Rb, int Cb>
inline typename std::enable_if<
    std::is_same<Ta, var>::value || std::is_same<Tb, var>::value, var>::type
trace_quad_form(const Eigen::Matrix<Ta, Ra, Ca>& A,
                const Eigen::Matrix<Tb, Rb, Cb>& B) {
  check_square("trace_quad_form", "A", A);
  check_multiplicable("trace_quad_form", "A", A, "B", B);

  internal::trace_quad_form_vari_alloc<Ta, Ra, Ca, Tb, Rb, Cb>* baseVari
      = new internal::trace_quad_form_vari_alloc<Ta, Ra, Ca, Tb, Rb, Cb>(A, B);

  return var(
      new internal::trace_quad_form_vari<Ta, Ra, Ca, Tb, Rb, Cb>(baseVari));
}

}  // namespace math
}  // namespace stan
#endif
