#ifndef STAN_MATH_REV_MAT_FUN_DETERMINANT_HPP
#define STAN_MATH_REV_MAT_FUN_DETERMINANT_HPP

#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/rev/core.hpp>
#include <stan/math/rev/mat/fun/typedefs.hpp>
#include <vector>

namespace stan {
namespace math {

namespace internal {
template <int R, int C>
class determinant_vari : public vari {
  int rows_;
  int cols_;
  double* A_;
  vari** adjARef_;

 public:
  explicit determinant_vari(const Eigen::Matrix<var, R, C>& A)
      : vari(determinant_vari_calc(A)),
        rows_(A.rows()),
        cols_(A.cols()),
        A_(reinterpret_cast<double*>(ChainableStack::instance().memalloc_.alloc(
            sizeof(double) * A.rows() * A.cols()))),
        adjARef_(
            reinterpret_cast<vari**>(ChainableStack::instance().memalloc_.alloc(
                sizeof(vari*) * A.rows() * A.cols()))) {
    size_t pos = 0;
    for (size_type j = 0; j < cols_; j++) {
      for (size_type i = 0; i < rows_; i++) {
        A_[pos] = A(i, j).val();
        adjARef_[pos++] = A(i, j).vi_;
      }
    }
  }
  static double determinant_vari_calc(const Eigen::Matrix<var, R, C>& A) {
    Eigen::Matrix<double, R, C> Ad(A.rows(), A.cols());
    for (size_type j = 0; j < A.rows(); j++)
      for (size_type i = 0; i < A.cols(); i++)
        Ad(i, j) = A(i, j).val();
    return Ad.determinant();
  }
  virtual void chain() {
    using Eigen::Map;
    using Eigen::Matrix;
    Matrix<double, R, C> adjA(rows_, cols_);
    adjA = (adj_ * val_)
           * Map<Matrix<double, R, C> >(A_, rows_, cols_).inverse().transpose();
    size_t pos = 0;
    for (size_type j = 0; j < cols_; j++) {
      for (size_type i = 0; i < rows_; i++) {
        adjARef_[pos++]->adj_ += adjA(i, j);
      }
    }
  }
};
}  // namespace internal

template <int R, int C>
inline var determinant(const Eigen::Matrix<var, R, C>& m) {
  check_square("determinant", "m", m);
  return var(new internal::determinant_vari<R, C>(m));
}

}  // namespace math
}  // namespace stan
#endif
