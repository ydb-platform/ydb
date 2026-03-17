#ifndef STAN_MATH_REV_MAT_FUN_LOG_DETERMINANT_LDLT_HPP
#define STAN_MATH_REV_MAT_FUN_LOG_DETERMINANT_LDLT_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/rev/core.hpp>
#include <stan/math/rev/mat/fun/LDLT_alloc.hpp>
#include <stan/math/rev/mat/fun/LDLT_factor.hpp>

namespace stan {
namespace math {
namespace internal {

/**
 * Returns the log det of the matrix whose LDLT factorization is given
 * See The Matrix Cookbook's chapter on Derivatives of a Determinant
 * In this case, it is just the inverse of the underlying matrix
 * @param A, which is a LDLT_factor
 * @return ln(det(A))
 * @throws never
 */

template <int R, int C>
class log_det_ldlt_vari : public vari {
 public:
  explicit log_det_ldlt_vari(const LDLT_factor<var, R, C> &A)
      : vari(A.alloc_->log_abs_det()), alloc_ldlt_(A.alloc_) {}

  virtual void chain() {
    Eigen::Matrix<double, R, C> invA;

    // If we start computing Jacobians, this may be a bit inefficient
    invA.setIdentity(alloc_ldlt_->N_, alloc_ldlt_->N_);
    alloc_ldlt_->ldlt_.solveInPlace(invA);

    for (size_t j = 0; j < alloc_ldlt_->N_; j++) {
      for (size_t i = 0; i < alloc_ldlt_->N_; i++) {
        alloc_ldlt_->variA_(i, j)->adj_ += adj_ * invA(i, j);
      }
    }
  }

  const LDLT_alloc<R, C> *alloc_ldlt_;
};
}  // namespace internal

template <int R, int C>
var log_determinant_ldlt(LDLT_factor<var, R, C> &A) {
  return var(new internal::log_det_ldlt_vari<R, C>(A));
}

}  // namespace math
}  // namespace stan
#endif
