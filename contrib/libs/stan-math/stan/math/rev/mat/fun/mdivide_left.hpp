#ifndef STAN_MATH_REV_MAT_FUN_MDIVIDE_LEFT_HPP
#define STAN_MATH_REV_MAT_FUN_MDIVIDE_LEFT_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/rev/core.hpp>
#include <stan/math/rev/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <vector>

namespace stan {
namespace math {

namespace internal {
template <int R1, int C1, int R2, int C2>
class mdivide_left_vv_vari : public vari {
 public:
  int M_;  // A.rows() = A.cols() = B.rows()
  int N_;  // B.cols()
  double *A_;
  double *C_;
  vari **variRefA_;
  vari **variRefB_;
  vari **variRefC_;

  mdivide_left_vv_vari(const Eigen::Matrix<var, R1, C1> &A,
                       const Eigen::Matrix<var, R2, C2> &B)
      : vari(0.0),
        M_(A.rows()),
        N_(B.cols()),
        A_(reinterpret_cast<double *>(
            ChainableStack::instance().memalloc_.alloc(sizeof(double) * A.rows()
                                                       * A.cols()))),
        C_(reinterpret_cast<double *>(
            ChainableStack::instance().memalloc_.alloc(sizeof(double) * B.rows()
                                                       * B.cols()))),
        variRefA_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * A.rows()
                                                       * A.cols()))),
        variRefB_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * B.rows()
                                                       * B.cols()))),
        variRefC_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * B.rows()
                                                       * B.cols()))) {
    using Eigen::Map;
    using Eigen::Matrix;

    size_t pos = 0;
    for (size_type j = 0; j < M_; j++) {
      for (size_type i = 0; i < M_; i++) {
        variRefA_[pos] = A(i, j).vi_;
        A_[pos++] = A(i, j).val();
      }
    }

    pos = 0;
    for (size_type j = 0; j < N_; j++) {
      for (size_type i = 0; i < M_; i++) {
        variRefB_[pos] = B(i, j).vi_;
        C_[pos++] = B(i, j).val();
      }
    }

    Matrix<double, R1, C2> C(M_, N_);
    C = Map<Matrix<double, R1, C2> >(C_, M_, N_);

    C = Map<Matrix<double, R1, C1> >(A_, M_, M_).colPivHouseholderQr().solve(C);

    pos = 0;
    for (size_type j = 0; j < N_; j++) {
      for (size_type i = 0; i < M_; i++) {
        C_[pos] = C(i, j);
        variRefC_[pos] = new vari(C_[pos], false);
        pos++;
      }
    }
  }

  virtual void chain() {
    using Eigen::Map;
    using Eigen::Matrix;
    Eigen::Matrix<double, R1, C1> adjA(M_, M_);
    Eigen::Matrix<double, R2, C2> adjB(M_, N_);
    Eigen::Matrix<double, R1, C2> adjC(M_, N_);

    size_t pos = 0;
    for (size_type j = 0; j < adjC.cols(); j++)
      for (size_type i = 0; i < adjC.rows(); i++)
        adjC(i, j) = variRefC_[pos++]->adj_;

    adjB = Map<Matrix<double, R1, C1> >(A_, M_, M_)
               .transpose()
               .colPivHouseholderQr()
               .solve(adjC);
    adjA.noalias()
        = -adjB * Map<Matrix<double, R1, C2> >(C_, M_, N_).transpose();

    pos = 0;
    for (size_type j = 0; j < adjA.cols(); j++)
      for (size_type i = 0; i < adjA.rows(); i++)
        variRefA_[pos++]->adj_ += adjA(i, j);

    pos = 0;
    for (size_type j = 0; j < adjB.cols(); j++)
      for (size_type i = 0; i < adjB.rows(); i++)
        variRefB_[pos++]->adj_ += adjB(i, j);
  }
};

template <int R1, int C1, int R2, int C2>
class mdivide_left_dv_vari : public vari {
 public:
  int M_;  // A.rows() = A.cols() = B.rows()
  int N_;  // B.cols()
  double *A_;
  double *C_;
  vari **variRefB_;
  vari **variRefC_;

  mdivide_left_dv_vari(const Eigen::Matrix<double, R1, C1> &A,
                       const Eigen::Matrix<var, R2, C2> &B)
      : vari(0.0),
        M_(A.rows()),
        N_(B.cols()),
        A_(reinterpret_cast<double *>(
            ChainableStack::instance().memalloc_.alloc(sizeof(double) * A.rows()
                                                       * A.cols()))),
        C_(reinterpret_cast<double *>(
            ChainableStack::instance().memalloc_.alloc(sizeof(double) * B.rows()
                                                       * B.cols()))),
        variRefB_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * B.rows()
                                                       * B.cols()))),
        variRefC_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * B.rows()
                                                       * B.cols()))) {
    using Eigen::Map;
    using Eigen::Matrix;

    size_t pos = 0;
    for (size_type j = 0; j < M_; j++) {
      for (size_type i = 0; i < M_; i++) {
        A_[pos++] = A(i, j);
      }
    }

    pos = 0;
    for (size_type j = 0; j < N_; j++) {
      for (size_type i = 0; i < M_; i++) {
        variRefB_[pos] = B(i, j).vi_;
        C_[pos++] = B(i, j).val();
      }
    }

    Matrix<double, R1, C2> C(M_, N_);
    C = Map<Matrix<double, R1, C2> >(C_, M_, N_);

    C = Map<Matrix<double, R1, C1> >(A_, M_, M_).colPivHouseholderQr().solve(C);

    pos = 0;
    for (size_type j = 0; j < N_; j++) {
      for (size_type i = 0; i < M_; i++) {
        C_[pos] = C(i, j);
        variRefC_[pos] = new vari(C_[pos], false);
        pos++;
      }
    }
  }

  virtual void chain() {
    using Eigen::Map;
    using Eigen::Matrix;
    Eigen::Matrix<double, R2, C2> adjB(M_, N_);
    Eigen::Matrix<double, R1, C2> adjC(M_, N_);

    size_t pos = 0;
    for (size_type j = 0; j < adjC.cols(); j++)
      for (size_type i = 0; i < adjC.rows(); i++)
        adjC(i, j) = variRefC_[pos++]->adj_;

    adjB = Map<Matrix<double, R1, C1> >(A_, M_, M_)
               .transpose()
               .colPivHouseholderQr()
               .solve(adjC);

    pos = 0;
    for (size_type j = 0; j < adjB.cols(); j++)
      for (size_type i = 0; i < adjB.rows(); i++)
        variRefB_[pos++]->adj_ += adjB(i, j);
  }
};

template <int R1, int C1, int R2, int C2>
class mdivide_left_vd_vari : public vari {
 public:
  int M_;  // A.rows() = A.cols() = B.rows()
  int N_;  // B.cols()
  double *A_;
  double *C_;
  vari **variRefA_;
  vari **variRefC_;

  mdivide_left_vd_vari(const Eigen::Matrix<var, R1, C1> &A,
                       const Eigen::Matrix<double, R2, C2> &B)
      : vari(0.0),
        M_(A.rows()),
        N_(B.cols()),
        A_(reinterpret_cast<double *>(
            ChainableStack::instance().memalloc_.alloc(sizeof(double) * A.rows()
                                                       * A.cols()))),
        C_(reinterpret_cast<double *>(
            ChainableStack::instance().memalloc_.alloc(sizeof(double) * B.rows()
                                                       * B.cols()))),
        variRefA_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * A.rows()
                                                       * A.cols()))),
        variRefC_(reinterpret_cast<vari **>(
            ChainableStack::instance().memalloc_.alloc(sizeof(vari *) * B.rows()
                                                       * B.cols()))) {
    using Eigen::Map;
    using Eigen::Matrix;

    size_t pos = 0;
    for (size_type j = 0; j < M_; j++) {
      for (size_type i = 0; i < M_; i++) {
        variRefA_[pos] = A(i, j).vi_;
        A_[pos++] = A(i, j).val();
      }
    }

    Matrix<double, R1, C2> C(M_, N_);
    C = Map<Matrix<double, R1, C1> >(A_, M_, M_).colPivHouseholderQr().solve(B);

    pos = 0;
    for (size_type j = 0; j < N_; j++) {
      for (size_type i = 0; i < M_; i++) {
        C_[pos] = C(i, j);
        variRefC_[pos] = new vari(C_[pos], false);
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

    // FIXME: add .noalias() to LHS
    adjA = -Map<Matrix<double, R1, C1> >(A_, M_, M_)
                .transpose()
                .colPivHouseholderQr()
                .solve(adjC
                       * Map<Matrix<double, R1, C2> >(C_, M_, N_).transpose());

    pos = 0;
    for (size_type j = 0; j < adjA.cols(); j++)
      for (size_type i = 0; i < adjA.rows(); i++)
        variRefA_[pos++]->adj_ += adjA(i, j);
  }
};
}  // namespace internal

template <int R1, int C1, int R2, int C2>
inline Eigen::Matrix<var, R1, C2> mdivide_left(
    const Eigen::Matrix<var, R1, C1> &A, const Eigen::Matrix<var, R2, C2> &b) {
  Eigen::Matrix<var, R1, C2> res(b.rows(), b.cols());

  check_square("mdivide_left", "A", A);
  check_multiplicable("mdivide_left", "A", A, "b", b);

  // NOTE: this is not a memory leak, this vari is used in the
  // expression graph to evaluate the adjoint, but is not needed
  // for the returned matrix.  Memory will be cleaned up with the
  // arena allocator.
  internal::mdivide_left_vv_vari<R1, C1, R2, C2> *baseVari
      = new internal::mdivide_left_vv_vari<R1, C1, R2, C2>(A, b);

  size_t pos = 0;
  for (size_type j = 0; j < res.cols(); j++)
    for (size_type i = 0; i < res.rows(); i++)
      res(i, j).vi_ = baseVari->variRefC_[pos++];

  return res;
}

template <int R1, int C1, int R2, int C2>
inline Eigen::Matrix<var, R1, C2> mdivide_left(
    const Eigen::Matrix<var, R1, C1> &A,
    const Eigen::Matrix<double, R2, C2> &b) {
  Eigen::Matrix<var, R1, C2> res(b.rows(), b.cols());

  check_square("mdivide_left", "A", A);
  check_multiplicable("mdivide_left", "A", A, "b", b);

  // NOTE: this is not a memory leak, this vari is used in the
  // expression graph to evaluate the adjoint, but is not needed
  // for the returned matrix.  Memory will be cleaned up with the
  // arena allocator.
  internal::mdivide_left_vd_vari<R1, C1, R2, C2> *baseVari
      = new internal::mdivide_left_vd_vari<R1, C1, R2, C2>(A, b);

  size_t pos = 0;
  for (size_type j = 0; j < res.cols(); j++)
    for (size_type i = 0; i < res.rows(); i++)
      res(i, j).vi_ = baseVari->variRefC_[pos++];

  return res;
}

template <int R1, int C1, int R2, int C2>
inline Eigen::Matrix<var, R1, C2> mdivide_left(
    const Eigen::Matrix<double, R1, C1> &A,
    const Eigen::Matrix<var, R2, C2> &b) {
  Eigen::Matrix<var, R1, C2> res(b.rows(), b.cols());

  check_square("mdivide_left", "A", A);
  check_multiplicable("mdivide_left", "A", A, "b", b);

  // NOTE: this is not a memory leak, this vari is used in the
  // expression graph to evaluate the adjoint, but is not needed
  // for the returned matrix.  Memory will be cleaned up with the
  // arena allocator.
  internal::mdivide_left_dv_vari<R1, C1, R2, C2> *baseVari
      = new internal::mdivide_left_dv_vari<R1, C1, R2, C2>(A, b);

  size_t pos = 0;
  for (size_type j = 0; j < res.cols(); j++)
    for (size_type i = 0; i < res.rows(); i++)
      res(i, j).vi_ = baseVari->variRefC_[pos++];

  return res;
}

}  // namespace math
}  // namespace stan
#endif
