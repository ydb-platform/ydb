#ifndef STAN_MATH_REV_MAT_FUN_MDIVIDE_LEFT_SPD_HPP
#define STAN_MATH_REV_MAT_FUN_MDIVIDE_LEFT_SPD_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/rev/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <vector>

namespace stan {
namespace math {

namespace internal {
template <int R1, int C1, int R2, int C2>
class mdivide_left_spd_alloc : public chainable_alloc {
 public:
  virtual ~mdivide_left_spd_alloc() {}

  Eigen::LLT<Eigen::Matrix<double, R1, C1> > llt_;
  Eigen::Matrix<double, R2, C2> C_;
};

template <int R1, int C1, int R2, int C2>
class mdivide_left_spd_vv_vari : public vari {
 public:
  int M_;  // A.rows() = A.cols() = B.rows()
  int N_;  // B.cols()
  vari **variRefA_;
  vari **variRefB_;
  vari **variRefC_;
  mdivide_left_spd_alloc<R1, C1, R2, C2> *alloc_;

  mdivide_left_spd_vv_vari(const Eigen::Matrix<var, R1, C1> &A,
                           const Eigen::Matrix<var, R2, C2> &B)
      : vari(0.0),
        M_(A.rows()),
        N_(B.cols()),
        variRefA_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * A.rows()
                                                       * A.cols()))),
        variRefB_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * B.rows()
                                                       * B.cols()))),
        variRefC_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * B.rows()
                                                       * B.cols()))),
        alloc_(new mdivide_left_spd_alloc<R1, C1, R2, C2>()) {
    using Eigen::Map;
    using Eigen::Matrix;

    Matrix<double, R1, C1> Ad(A.rows(), A.cols());

    size_t pos = 0;
    for (size_type j = 0; j < M_; j++) {
      for (size_type i = 0; i < M_; i++) {
        variRefA_[pos] = A(i, j).vi_;
        Ad(i, j) = A(i, j).val();
        pos++;
      }
    }

    pos = 0;
    alloc_->C_.resize(M_, N_);
    for (size_type j = 0; j < N_; j++) {
      for (size_type i = 0; i < M_; i++) {
        variRefB_[pos] = B(i, j).vi_;
        alloc_->C_(i, j) = B(i, j).val();
        pos++;
      }
    }

    alloc_->llt_ = Ad.llt();
    alloc_->llt_.solveInPlace(alloc_->C_);

    pos = 0;
    for (size_type j = 0; j < N_; j++) {
      for (size_type i = 0; i < M_; i++) {
        variRefC_[pos] = new vari(alloc_->C_(i, j), false);
        pos++;
      }
    }
  }

  virtual void chain() {
    using Eigen::Map;
    using Eigen::Matrix;
    Eigen::Matrix<double, R1, C1> adjA(M_, M_);
    Eigen::Matrix<double, R2, C2> adjB(M_, N_);

    size_t pos = 0;
    for (size_type j = 0; j < N_; j++)
      for (size_type i = 0; i < M_; i++)
        adjB(i, j) = variRefC_[pos++]->adj_;

    alloc_->llt_.solveInPlace(adjB);
    adjA.noalias() = -adjB * alloc_->C_.transpose();

    pos = 0;
    for (size_type j = 0; j < M_; j++)
      for (size_type i = 0; i < M_; i++)
        variRefA_[pos++]->adj_ += adjA(i, j);

    pos = 0;
    for (size_type j = 0; j < N_; j++)
      for (size_type i = 0; i < M_; i++)
        variRefB_[pos++]->adj_ += adjB(i, j);
  }
};

template <int R1, int C1, int R2, int C2>
class mdivide_left_spd_dv_vari : public vari {
 public:
  int M_;  // A.rows() = A.cols() = B.rows()
  int N_;  // B.cols()
  vari **variRefB_;
  vari **variRefC_;
  mdivide_left_spd_alloc<R1, C1, R2, C2> *alloc_;

  mdivide_left_spd_dv_vari(const Eigen::Matrix<double, R1, C1> &A,
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
        alloc_(new mdivide_left_spd_alloc<R1, C1, R2, C2>()) {
    using Eigen::Map;
    using Eigen::Matrix;

    size_t pos = 0;
    alloc_->C_.resize(M_, N_);
    for (size_type j = 0; j < N_; j++) {
      for (size_type i = 0; i < M_; i++) {
        variRefB_[pos] = B(i, j).vi_;
        alloc_->C_(i, j) = B(i, j).val();
        pos++;
      }
    }

    alloc_->llt_ = A.llt();
    alloc_->llt_.solveInPlace(alloc_->C_);

    pos = 0;
    for (size_type j = 0; j < N_; j++) {
      for (size_type i = 0; i < M_; i++) {
        variRefC_[pos] = new vari(alloc_->C_(i, j), false);
        pos++;
      }
    }
  }

  virtual void chain() {
    using Eigen::Map;
    using Eigen::Matrix;
    Eigen::Matrix<double, R2, C2> adjB(M_, N_);

    size_t pos = 0;
    for (size_type j = 0; j < adjB.cols(); j++)
      for (size_type i = 0; i < adjB.rows(); i++)
        adjB(i, j) = variRefC_[pos++]->adj_;

    alloc_->llt_.solveInPlace(adjB);

    pos = 0;
    for (size_type j = 0; j < adjB.cols(); j++)
      for (size_type i = 0; i < adjB.rows(); i++)
        variRefB_[pos++]->adj_ += adjB(i, j);
  }
};

template <int R1, int C1, int R2, int C2>
class mdivide_left_spd_vd_vari : public vari {
 public:
  int M_;  // A.rows() = A.cols() = B.rows()
  int N_;  // B.cols()
  vari **variRefA_;
  vari **variRefC_;
  mdivide_left_spd_alloc<R1, C1, R2, C2> *alloc_;

  mdivide_left_spd_vd_vari(const Eigen::Matrix<var, R1, C1> &A,
                           const Eigen::Matrix<double, R2, C2> &B)
      : vari(0.0),
        M_(A.rows()),
        N_(B.cols()),
        variRefA_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * A.rows()
                                                       * A.cols()))),
        variRefC_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * B.rows()
                                                       * B.cols()))),
        alloc_(new mdivide_left_spd_alloc<R1, C1, R2, C2>()) {
    using Eigen::Map;
    using Eigen::Matrix;

    Matrix<double, R1, C1> Ad(A.rows(), A.cols());

    size_t pos = 0;
    for (size_type j = 0; j < M_; j++) {
      for (size_type i = 0; i < M_; i++) {
        variRefA_[pos] = A(i, j).vi_;
        Ad(i, j) = A(i, j).val();
        pos++;
      }
    }

    alloc_->llt_ = Ad.llt();
    alloc_->C_ = alloc_->llt_.solve(B);

    pos = 0;
    for (size_type j = 0; j < N_; j++) {
      for (size_type i = 0; i < M_; i++) {
        variRefC_[pos] = new vari(alloc_->C_(i, j), false);
        pos++;
      }
    }
  }

  virtual void chain() {
    using Eigen::Map;
    using Eigen::Matrix;
    Eigen::Matrix<double, R1, C1> adjA(M_, M_);
    Eigen::Matrix<double, R1, C2> adjC(M_, N_);

    size_t pos = 0;
    for (size_type j = 0; j < adjC.cols(); j++)
      for (size_type i = 0; i < adjC.rows(); i++)
        adjC(i, j) = variRefC_[pos++]->adj_;

    adjA = -alloc_->llt_.solve(adjC * alloc_->C_.transpose());

    pos = 0;
    for (size_type j = 0; j < adjA.cols(); j++)
      for (size_type i = 0; i < adjA.rows(); i++)
        variRefA_[pos++]->adj_ += adjA(i, j);
  }
};
}  // namespace internal

template <int R1, int C1, int R2, int C2>
inline Eigen::Matrix<var, R1, C2> mdivide_left_spd(
    const Eigen::Matrix<var, R1, C1> &A, const Eigen::Matrix<var, R2, C2> &b) {
  Eigen::Matrix<var, R1, C2> res(b.rows(), b.cols());

  check_square("mdivide_left_spd", "A", A);
  check_multiplicable("mdivide_left_spd", "A", A, "b", b);

  // NOTE: this is not a memory leak, this vari is used in the
  // expression graph to evaluate the adjoint, but is not needed
  // for the returned matrix.  Memory will be cleaned up with the
  // arena allocator.
  internal::mdivide_left_spd_vv_vari<R1, C1, R2, C2> *baseVari
      = new internal::mdivide_left_spd_vv_vari<R1, C1, R2, C2>(A, b);

  size_t pos = 0;
  for (size_type j = 0; j < res.cols(); j++)
    for (size_type i = 0; i < res.rows(); i++)
      res(i, j).vi_ = baseVari->variRefC_[pos++];

  return res;
}

template <int R1, int C1, int R2, int C2>
inline Eigen::Matrix<var, R1, C2> mdivide_left_spd(
    const Eigen::Matrix<var, R1, C1> &A,
    const Eigen::Matrix<double, R2, C2> &b) {
  Eigen::Matrix<var, R1, C2> res(b.rows(), b.cols());

  check_square("mdivide_left_spd", "A", A);
  check_multiplicable("mdivide_left_spd", "A", A, "b", b);

  // NOTE: this is not a memory leak, this vari is used in the
  // expression graph to evaluate the adjoint, but is not needed
  // for the returned matrix.  Memory will be cleaned up with the
  // arena allocator.
  internal::mdivide_left_spd_vd_vari<R1, C1, R2, C2> *baseVari
      = new internal::mdivide_left_spd_vd_vari<R1, C1, R2, C2>(A, b);

  size_t pos = 0;
  for (size_type j = 0; j < res.cols(); j++)
    for (size_type i = 0; i < res.rows(); i++)
      res(i, j).vi_ = baseVari->variRefC_[pos++];

  return res;
}

template <int R1, int C1, int R2, int C2>
inline Eigen::Matrix<var, R1, C2> mdivide_left_spd(
    const Eigen::Matrix<double, R1, C1> &A,
    const Eigen::Matrix<var, R2, C2> &b) {
  Eigen::Matrix<var, R1, C2> res(b.rows(), b.cols());

  check_square("mdivide_left_spd", "A", A);
  check_multiplicable("mdivide_left_spd", "A", A, "b", b);

  // NOTE: this is not a memory leak, this vari is used in the
  // expression graph to evaluate the adjoint, but is not needed
  // for the returned matrix.  Memory will be cleaned up with the
  // arena allocator.
  internal::mdivide_left_spd_dv_vari<R1, C1, R2, C2> *baseVari
      = new internal::mdivide_left_spd_dv_vari<R1, C1, R2, C2>(A, b);

  size_t pos = 0;
  for (size_type j = 0; j < res.cols(); j++)
    for (size_type i = 0; i < res.rows(); i++)
      res(i, j).vi_ = baseVari->variRefC_[pos++];

  return res;
}

}  // namespace math
}  // namespace stan
#endif
