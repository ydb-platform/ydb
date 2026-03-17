#ifndef STAN_MATH_REV_MAT_FUN_MDIVIDE_LEFT_LDLT_HPP
#define STAN_MATH_REV_MAT_FUN_MDIVIDE_LEFT_LDLT_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/rev/mat/fun/LDLT_alloc.hpp>
#include <stan/math/rev/mat/fun/LDLT_factor.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>

namespace stan {
namespace math {
namespace internal {
template <int R1, int C1, int R2, int C2>
class mdivide_left_ldlt_alloc : public chainable_alloc {
 public:
  virtual ~mdivide_left_ldlt_alloc() {}

  /**
   * This share_ptr is used to prevent copying the LDLT factorizations
   * for mdivide_left_ldlt(ldltA, b) when ldltA is a LDLT_factor<double>.
   * The pointer is shared with the LDLT_factor<double> class.
   **/
  boost::shared_ptr<Eigen::LDLT<Eigen::Matrix<double, R1, C1> > > ldltP_;
  Eigen::Matrix<double, R2, C2> C_;
};

/**
 * The vari for mdivide_left_ldlt(A, b) which handles the chain() call
 * for all elements of the result.  This vari follows the pattern
 * used in the other matrix operations where there is one "master"
 * vari whose value is never used and a large number of "slave" varis
 * whose chain() functions are never called because their adjoints are
 * set by the "mater" vari.
 *
 * This class handles the var/var case.
 **/
template <int R1, int C1, int R2, int C2>
class mdivide_left_ldlt_vv_vari : public vari {
 public:
  int M_;  // A.rows() = A.cols() = B.rows()
  int N_;  // B.cols()
  vari **variRefB_;
  vari **variRefC_;
  mdivide_left_ldlt_alloc<R1, C1, R2, C2> *alloc_;
  const LDLT_alloc<R1, C1> *alloc_ldlt_;

  mdivide_left_ldlt_vv_vari(const LDLT_factor<var, R1, C1> &A,
                            const Eigen::Matrix<var, R2, C2> &B)
      : vari(0.0),
        M_(A.rows()),
        N_(B.cols()),
        variRefB_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * B.rows()
                                                       * B.cols()))),
        variRefC_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * B.rows()
                                                       * B.cols()))),
        alloc_(new mdivide_left_ldlt_alloc<R1, C1, R2, C2>()),
        alloc_ldlt_(A.alloc_) {
    int pos = 0;
    alloc_->C_.resize(M_, N_);
    for (int j = 0; j < N_; j++) {
      for (int i = 0; i < M_; i++) {
        variRefB_[pos] = B(i, j).vi_;
        alloc_->C_(i, j) = B(i, j).val();
        pos++;
      }
    }

    alloc_ldlt_->ldlt_.solveInPlace(alloc_->C_);

    pos = 0;
    for (int j = 0; j < N_; j++) {
      for (int i = 0; i < M_; i++) {
        variRefC_[pos] = new vari(alloc_->C_(i, j), false);
        pos++;
      }
    }
  }

  virtual void chain() {
    Eigen::Matrix<double, R1, C1> adjA(M_, M_);
    Eigen::Matrix<double, R2, C2> adjB(M_, N_);

    int pos = 0;
    for (int j = 0; j < N_; j++)
      for (int i = 0; i < M_; i++)
        adjB(i, j) = variRefC_[pos++]->adj_;

    alloc_ldlt_->ldlt_.solveInPlace(adjB);
    adjA.noalias() = -adjB * alloc_->C_.transpose();

    for (int j = 0; j < M_; j++)
      for (int i = 0; i < M_; i++)
        alloc_ldlt_->variA_(i, j)->adj_ += adjA(i, j);

    pos = 0;
    for (int j = 0; j < N_; j++)
      for (int i = 0; i < M_; i++)
        variRefB_[pos++]->adj_ += adjB(i, j);
  }
};

/**
 * The vari for mdivide_left_ldlt(A, b) which handles the chain() call
 * for all elements of the result.  This vari follows the pattern
 * used in the other matrix operations where there is one "master"
 * vari whose value is never used and a large number of "slave" varis
 * whose chain() functions are never called because their adjoints are
 * set by the "mater" vari.
 *
 * This class handles the double/var case.
 **/
template <int R1, int C1, int R2, int C2>
class mdivide_left_ldlt_dv_vari : public vari {
 public:
  int M_;  // A.rows() = A.cols() = B.rows()
  int N_;  // B.cols()
  vari **variRefB_;
  vari **variRefC_;
  mdivide_left_ldlt_alloc<R1, C1, R2, C2> *alloc_;

  mdivide_left_ldlt_dv_vari(const LDLT_factor<double, R1, C1> &A,
                            const Eigen::Matrix<var, R2, C2> &B)
      : vari(0.0),
        M_(A.rows()),
        N_(B.cols()),
        variRefB_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * B.rows()
                                                       * B.cols()))),
        variRefC_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * B.rows()
                                                       * B.cols()))),
        alloc_(new mdivide_left_ldlt_alloc<R1, C1, R2, C2>()) {
    using Eigen::Map;
    using Eigen::Matrix;

    int pos = 0;
    alloc_->C_.resize(M_, N_);
    for (int j = 0; j < N_; j++) {
      for (int i = 0; i < M_; i++) {
        variRefB_[pos] = B(i, j).vi_;
        alloc_->C_(i, j) = B(i, j).val();
        pos++;
      }
    }

    alloc_->ldltP_ = A.ldltP_;
    alloc_->ldltP_->solveInPlace(alloc_->C_);

    pos = 0;
    for (int j = 0; j < N_; j++) {
      for (int i = 0; i < M_; i++) {
        variRefC_[pos] = new vari(alloc_->C_(i, j), false);
        pos++;
      }
    }
  }

  virtual void chain() {
    Eigen::Matrix<double, R2, C2> adjB(M_, N_);

    int pos = 0;
    for (int j = 0; j < adjB.cols(); j++)
      for (int i = 0; i < adjB.rows(); i++)
        adjB(i, j) = variRefC_[pos++]->adj_;

    alloc_->ldltP_->solveInPlace(adjB);

    pos = 0;
    for (int j = 0; j < adjB.cols(); j++)
      for (int i = 0; i < adjB.rows(); i++)
        variRefB_[pos++]->adj_ += adjB(i, j);
  }
};

/**
 * The vari for mdivide_left_ldlt(A, b) which handles the chain() call
 * for all elements of the result.  This vari follows the pattern
 * used in the other matrix operations where there is one "master"
 * vari whose value is never used and a large number of "slave" varis
 * whose chain() functions are never called because their adjoints are
 * set by the "mater" vari.
 *
 * This class handles the var/double case.
 **/
template <int R1, int C1, int R2, int C2>
class mdivide_left_ldlt_vd_vari : public vari {
 public:
  int M_;  // A.rows() = A.cols() = B.rows()
  int N_;  // B.cols()
  vari **variRefC_;
  mdivide_left_ldlt_alloc<R1, C1, R2, C2> *alloc_;
  const LDLT_alloc<R1, C1> *alloc_ldlt_;

  mdivide_left_ldlt_vd_vari(const LDLT_factor<var, R1, C1> &A,
                            const Eigen::Matrix<double, R2, C2> &B)
      : vari(0.0),
        M_(A.rows()),
        N_(B.cols()),
        variRefC_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * B.rows()
                                                       * B.cols()))),
        alloc_(new mdivide_left_ldlt_alloc<R1, C1, R2, C2>()),
        alloc_ldlt_(A.alloc_) {
    alloc_->C_ = B;
    alloc_ldlt_->ldlt_.solveInPlace(alloc_->C_);

    int pos = 0;
    for (int j = 0; j < N_; j++) {
      for (int i = 0; i < M_; i++) {
        variRefC_[pos] = new vari(alloc_->C_(i, j), false);
        pos++;
      }
    }
  }

  virtual void chain() {
    Eigen::Matrix<double, R1, C1> adjA(M_, M_);
    Eigen::Matrix<double, R1, C2> adjC(M_, N_);

    int pos = 0;
    for (int j = 0; j < adjC.cols(); j++)
      for (int i = 0; i < adjC.rows(); i++)
        adjC(i, j) = variRefC_[pos++]->adj_;

    adjA = -alloc_ldlt_->ldlt_.solve(adjC * alloc_->C_.transpose());

    for (int j = 0; j < adjA.cols(); j++)
      for (int i = 0; i < adjA.rows(); i++)
        alloc_ldlt_->variA_(i, j)->adj_ += adjA(i, j);
  }
};
}  // namespace internal

/**
 * Returns the solution of the system Ax=b given an LDLT_factor of A
 * @param A LDLT_factor
 * @param b Right hand side matrix or vector.
 * @return x = b A^-1, solution of the linear system.
 * @throws std::domain_error if rows of b don't match the size of A.
 */
template <int R1, int C1, int R2, int C2>
inline Eigen::Matrix<var, R1, C2> mdivide_left_ldlt(
    const LDLT_factor<var, R1, C1> &A, const Eigen::Matrix<var, R2, C2> &b) {
  Eigen::Matrix<var, R1, C2> res(b.rows(), b.cols());

  check_multiplicable("mdivide_left_ldlt", "A", A, "b", b);

  internal::mdivide_left_ldlt_vv_vari<R1, C1, R2, C2> *baseVari
      = new internal::mdivide_left_ldlt_vv_vari<R1, C1, R2, C2>(A, b);

  int pos = 0;
  for (int j = 0; j < res.cols(); j++)
    for (int i = 0; i < res.rows(); i++)
      res(i, j).vi_ = baseVari->variRefC_[pos++];

  return res;
}

/**
 * Returns the solution of the system Ax=b given an LDLT_factor of A
 * @param A LDLT_factor
 * @param b Right hand side matrix or vector.
 * @return x = b A^-1, solution of the linear system.
 * @throws std::domain_error if rows of b don't match the size of A.
 */
template <int R1, int C1, int R2, int C2>
inline Eigen::Matrix<var, R1, C2> mdivide_left_ldlt(
    const LDLT_factor<var, R1, C1> &A, const Eigen::Matrix<double, R2, C2> &b) {
  Eigen::Matrix<var, R1, C2> res(b.rows(), b.cols());

  check_multiplicable("mdivide_left_ldlt", "A", A, "b", b);

  internal::mdivide_left_ldlt_vd_vari<R1, C1, R2, C2> *baseVari
      = new internal::mdivide_left_ldlt_vd_vari<R1, C1, R2, C2>(A, b);

  int pos = 0;
  for (int j = 0; j < res.cols(); j++)
    for (int i = 0; i < res.rows(); i++)
      res(i, j).vi_ = baseVari->variRefC_[pos++];

  return res;
}

/**
 * Returns the solution of the system Ax=b given an LDLT_factor of A
 * @param A LDLT_factor
 * @param b Right hand side matrix or vector.
 * @return x = b A^-1, solution of the linear system.
 * @throws std::domain_error if rows of b don't match the size of A.
 */
template <int R1, int C1, int R2, int C2>
inline Eigen::Matrix<var, R1, C2> mdivide_left_ldlt(
    const LDLT_factor<double, R1, C1> &A, const Eigen::Matrix<var, R2, C2> &b) {
  Eigen::Matrix<var, R1, C2> res(b.rows(), b.cols());

  check_multiplicable("mdivide_left_ldlt", "A", A, "b", b);

  internal::mdivide_left_ldlt_dv_vari<R1, C1, R2, C2> *baseVari
      = new internal::mdivide_left_ldlt_dv_vari<R1, C1, R2, C2>(A, b);

  int pos = 0;
  for (int j = 0; j < res.cols(); j++)
    for (int i = 0; i < res.rows(); i++)
      res(i, j).vi_ = baseVari->variRefC_[pos++];

  return res;
}

}  // namespace math
}  // namespace stan
#endif
