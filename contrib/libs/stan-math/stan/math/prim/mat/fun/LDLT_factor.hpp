#ifndef STAN_MATH_PRIM_MAT_FUN_LDLT_FACTOR_HPP
#define STAN_MATH_PRIM_MAT_FUN_LDLT_FACTOR_HPP

#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <boost/shared_ptr.hpp>

namespace stan {
namespace math {

/**
 * LDLT_factor is a thin wrapper on Eigen::LDLT to allow for
 * reusing factorizations and efficient autodiff of things like
 * log determinants and solutions to linear systems.
 *
 * Memory is allocated in the constructor and stored in a
 * <code>boost::shared_ptr</code>, which ensures that is freed
 * when the object is released.
 *
 * After the constructor and/or compute() is called users of
 * LDLT_factor are responsible for calling success() to
 * check whether the factorization has succeeded.  Use of an LDLT_factor
 * object (e.g., in mdivide_left_ldlt) is undefined if success() is false.
 *
 * It's usage pattern is:
 *
 * ~~~
 * Eigen::Matrix<T, R, C> A1, A2;
 *
 * LDLT_factor<T, R, C> ldlt_A1(A1);
 * LDLT_factor<T, R, C> ldlt_A2;
 * ldlt_A2.compute(A2);
 * ~~~
 *
 * The caller should check that ldlt_A1.success() and ldlt_A2.success()
 * are true or abort accordingly.  Alternatively, call check_ldlt_factor().
 *
 * Note that ldlt_A1 and ldlt_A2 are completely equivalent.  They simply
 * demonstrate two different ways to construct the factorization.
 *
 * The caller can use the LDLT_factor objects as needed.  For
 * instance
 *
 * ~~~
 * x1 = mdivide_left_ldlt(ldlt_A1, b1);
 * x2 = mdivide_right_ldlt(b2, ldlt_A2);
 *
 * d1 = log_determinant_ldlt(ldlt_A1);
 * d2 = log_determinant_ldlt(ldlt_A2);
 * ~~~
 *
 * This class is conceptually similar to the corresponding Eigen
 * class.  Any symmetric, positive-definite matrix A can be
 * decomposed as LDL' where L is unit lower-triangular and D is
 * diagonal with positive diagonal elements.
 *
 * @tparam T scalare type held in the matrix
 * @tparam R rows (as in Eigen)
 * @tparam C columns (as in Eigen)
 */
template <typename T, int R, int C>
class LDLT_factor {
 public:
  typedef Eigen::Matrix<T, Eigen::Dynamic, 1> vector_t;
  typedef Eigen::Matrix<T, R, C> matrix_t;
  typedef Eigen::LDLT<matrix_t> ldlt_t;
  typedef size_t size_type;
  typedef double value_type;

  LDLT_factor() : N_(0), ldltP_(new ldlt_t()) {}

  explicit LDLT_factor(const matrix_t& A) : N_(0), ldltP_(new ldlt_t()) {
    compute(A);
  }

  inline void compute(const matrix_t& A) {
    check_square("LDLT_factor", "A", A);
    N_ = A.rows();
    ldltP_->compute(A);
  }

  inline bool success() const {
    if (ldltP_->info() != Eigen::Success)
      return false;
    if (!(ldltP_->isPositive()))
      return false;
    vector_t ldltP_diag(ldltP_->vectorD());
    for (int i = 0; i < ldltP_diag.size(); ++i)
      if (ldltP_diag(i) <= 0 || is_nan(ldltP_diag(i)))
        return false;
    return true;
  }

  inline T log_abs_det() const { return ldltP_->vectorD().array().log().sum(); }

  inline void inverse(matrix_t& invA) const {
    invA.setIdentity(N_);
    ldltP_->solveInPlace(invA);
  }

#if EIGEN_VERSION_AT_LEAST(3, 3, 0)
  template <typename Rhs>
  inline const Eigen::Solve<ldlt_t, Rhs> solve(
      const Eigen::MatrixBase<Rhs>& b) const {
    return ldltP_->solve(b);
  }
#else
  template <typename Rhs>
  inline const Eigen::internal::solve_retval<ldlt_t, Rhs> solve(
      const Eigen::MatrixBase<Rhs>& b) const {
    return ldltP_->solve(b);
  }
#endif

  inline matrix_t solveRight(const matrix_t& B) const {
    return ldltP_->solve(B.transpose()).transpose();
  }

  inline vector_t vectorD() const { return ldltP_->vectorD(); }

  inline ldlt_t matrixLDLT() const { return ldltP_->matrixLDLT(); }

  inline size_t rows() const { return N_; }
  inline size_t cols() const { return N_; }

  size_t N_;
  boost::shared_ptr<ldlt_t> ldltP_;
};

}  // namespace math
}  // namespace stan
#endif
