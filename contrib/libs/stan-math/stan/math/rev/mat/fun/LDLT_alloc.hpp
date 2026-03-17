#ifndef STAN_MATH_REV_MAT_FUN_LDLT_ALLOC_HPP
#define STAN_MATH_REV_MAT_FUN_LDLT_ALLOC_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/rev/core.hpp>

namespace stan {
namespace math {
/**
 * This object stores the actual (double typed) LDLT factorization of
 * an Eigen::Matrix<var> along with pointers to its vari's which allow the
 * *ldlt_ functions to save memory.  It is derived from a chainable_alloc
 * object so that it is allocated on the stack but does not have a chain()
 * function called.
 *
 * This class should only be instantiated as part of an LDLT_factor object
 * and is only used in *ldlt_ functions.
 **/
template <int R, int C>
class LDLT_alloc : public chainable_alloc {
 public:
  LDLT_alloc() : N_(0) {}
  explicit LDLT_alloc(const Eigen::Matrix<var, R, C> &A) : N_(0) { compute(A); }

  /**
   * Compute the LDLT factorization and store pointers to the
   * vari's of the matrix entries to be used when chain() is
   * called elsewhere.
   **/
  inline void compute(const Eigen::Matrix<var, R, C> &A) {
    Eigen::Matrix<double, R, C> Ad(A.rows(), A.cols());

    N_ = A.rows();
    variA_.resize(A.rows(), A.cols());

    for (size_t j = 0; j < N_; j++) {
      for (size_t i = 0; i < N_; i++) {
        Ad(i, j) = A(i, j).val();
        variA_(i, j) = A(i, j).vi_;
      }
    }

    ldlt_.compute(Ad);
  }

  // Compute the log(abs(det(A))).  This is just a convenience function.
  inline double log_abs_det() const {
    return ldlt_.vectorD().array().log().sum();
  }

  size_t N_;
  Eigen::LDLT<Eigen::Matrix<double, R, C> > ldlt_;
  Eigen::Matrix<vari *, R, C> variA_;
};
}  // namespace math
}  // namespace stan
#endif
