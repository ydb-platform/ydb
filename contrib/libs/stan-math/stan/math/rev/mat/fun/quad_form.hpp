#ifndef STAN_MATH_REV_MAT_FUN_QUAD_FORM_HPP
#define STAN_MATH_REV_MAT_FUN_QUAD_FORM_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/rev/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/fun/value_of.hpp>
#include <stan/math/prim/mat/fun/quad_form.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/prim/mat/err/check_symmetric.hpp>
#include <type_traits>

namespace stan {
namespace math {

namespace internal {
template <typename Ta, int Ra, int Ca, typename Tb, int Rb, int Cb>
class quad_form_vari_alloc : public chainable_alloc {
 private:
  inline void compute(const Eigen::Matrix<double, Ra, Ca>& A,
                      const Eigen::Matrix<double, Rb, Cb>& B) {
    Eigen::Matrix<double, Cb, Cb> Cd(B.transpose() * A * B);
    for (int j = 0; j < C_.cols(); j++) {
      for (int i = 0; i < C_.rows(); i++) {
        if (sym_) {
          C_(i, j) = var(new vari(0.5 * (Cd(i, j) + Cd(j, i)), false));
        } else {
          C_(i, j) = var(new vari(Cd(i, j), false));
        }
      }
    }
  }

 public:
  quad_form_vari_alloc(const Eigen::Matrix<Ta, Ra, Ca>& A,
                       const Eigen::Matrix<Tb, Rb, Cb>& B,
                       bool symmetric = false)
      : A_(A), B_(B), C_(B_.cols(), B_.cols()), sym_(symmetric) {
    compute(value_of(A), value_of(B));
  }

  Eigen::Matrix<Ta, Ra, Ca> A_;
  Eigen::Matrix<Tb, Rb, Cb> B_;
  Eigen::Matrix<var, Cb, Cb> C_;
  bool sym_;
};

template <typename Ta, int Ra, int Ca, typename Tb, int Rb, int Cb>
class quad_form_vari : public vari {
 protected:
  inline void chainA(Eigen::Matrix<double, Ra, Ca>& A,
                     const Eigen::Matrix<double, Rb, Cb>& Bd,
                     const Eigen::Matrix<double, Cb, Cb>& adjC) {}
  inline void chainB(Eigen::Matrix<double, Rb, Cb>& B,
                     const Eigen::Matrix<double, Ra, Ca>& Ad,
                     const Eigen::Matrix<double, Rb, Cb>& Bd,
                     const Eigen::Matrix<double, Cb, Cb>& adjC) {}

  inline void chainA(Eigen::Matrix<var, Ra, Ca>& A,
                     const Eigen::Matrix<double, Rb, Cb>& Bd,
                     const Eigen::Matrix<double, Cb, Cb>& adjC) {
    Eigen::Matrix<double, Ra, Ca> adjA(Bd * adjC * Bd.transpose());
    for (int j = 0; j < A.cols(); j++) {
      for (int i = 0; i < A.rows(); i++) {
        A(i, j).vi_->adj_ += adjA(i, j);
      }
    }
  }
  inline void chainB(Eigen::Matrix<var, Rb, Cb>& B,
                     const Eigen::Matrix<double, Ra, Ca>& Ad,
                     const Eigen::Matrix<double, Rb, Cb>& Bd,
                     const Eigen::Matrix<double, Cb, Cb>& adjC) {
    Eigen::Matrix<double, Ra, Ca> adjB(Ad * Bd * adjC.transpose()
                                       + Ad.transpose() * Bd * adjC);
    for (int j = 0; j < B.cols(); j++)
      for (int i = 0; i < B.rows(); i++)
        B(i, j).vi_->adj_ += adjB(i, j);
  }

  inline void chainAB(Eigen::Matrix<Ta, Ra, Ca>& A,
                      Eigen::Matrix<Tb, Rb, Cb>& B,
                      const Eigen::Matrix<double, Ra, Ca>& Ad,
                      const Eigen::Matrix<double, Rb, Cb>& Bd,
                      const Eigen::Matrix<double, Cb, Cb>& adjC) {
    chainA(A, Bd, adjC);
    chainB(B, Ad, Bd, adjC);
  }

 public:
  quad_form_vari(const Eigen::Matrix<Ta, Ra, Ca>& A,
                 const Eigen::Matrix<Tb, Rb, Cb>& B, bool symmetric = false)
      : vari(0.0) {
    impl_ = new quad_form_vari_alloc<Ta, Ra, Ca, Tb, Rb, Cb>(A, B, symmetric);
  }

  virtual void chain() {
    Eigen::Matrix<double, Cb, Cb> adjC(impl_->C_.rows(), impl_->C_.cols());

    for (int j = 0; j < impl_->C_.cols(); j++)
      for (int i = 0; i < impl_->C_.rows(); i++)
        adjC(i, j) = impl_->C_(i, j).vi_->adj_;

    chainAB(impl_->A_, impl_->B_, value_of(impl_->A_), value_of(impl_->B_),
            adjC);
  }

  quad_form_vari_alloc<Ta, Ra, Ca, Tb, Rb, Cb>* impl_;
};
}  // namespace internal

template <typename Ta, int Ra, int Ca, typename Tb, int Rb, int Cb>
inline typename std::enable_if<std::is_same<Ta, var>::value
                                   || std::is_same<Tb, var>::value,
                               Eigen::Matrix<var, Cb, Cb> >::type
quad_form(const Eigen::Matrix<Ta, Ra, Ca>& A,
          const Eigen::Matrix<Tb, Rb, Cb>& B) {
  check_square("quad_form", "A", A);
  check_multiplicable("quad_form", "A", A, "B", B);

  internal::quad_form_vari<Ta, Ra, Ca, Tb, Rb, Cb>* baseVari
      = new internal::quad_form_vari<Ta, Ra, Ca, Tb, Rb, Cb>(A, B);

  return baseVari->impl_->C_;
}

template <typename Ta, int Ra, int Ca, typename Tb, int Rb>
inline typename std::enable_if<
    std::is_same<Ta, var>::value || std::is_same<Tb, var>::value, var>::type
quad_form(const Eigen::Matrix<Ta, Ra, Ca>& A,
          const Eigen::Matrix<Tb, Rb, 1>& B) {
  check_square("quad_form", "A", A);
  check_multiplicable("quad_form", "A", A, "B", B);

  internal::quad_form_vari<Ta, Ra, Ca, Tb, Rb, 1>* baseVari
      = new internal::quad_form_vari<Ta, Ra, Ca, Tb, Rb, 1>(A, B);

  return baseVari->impl_->C_(0, 0);
}

}  // namespace math
}  // namespace stan
#endif
