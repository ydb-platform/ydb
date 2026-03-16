#ifndef STAN_MATH_REV_MAT_FUN_SUM_HPP
#define STAN_MATH_REV_MAT_FUN_SUM_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/rev/core.hpp>
#include <stan/math/rev/arr/fun/sum.hpp>

namespace stan {
namespace math {

/**
 * Class for representing sums with constructors for Eigen.
 * The <code>chain()</code> method and member variables are
 * managed by the superclass <code>sum_v_vari</code>.
 */
class sum_eigen_v_vari : public sum_v_vari {
 protected:
  template <typename Derived>
  inline static double sum_of_val(const Eigen::DenseBase<Derived>& v) {
    double result = 0;
    for (int i = 0; i < v.size(); i++)
      result += v(i).vi_->val_;
    return result;
  }

 public:
  template <int R1, int C1>
  explicit sum_eigen_v_vari(const Eigen::Matrix<var, R1, C1>& v1)
      : sum_v_vari(
            sum_of_val(v1),
            reinterpret_cast<vari**>(ChainableStack::instance().memalloc_.alloc(
                v1.size() * sizeof(vari*))),
            v1.size()) {
    for (size_t i = 0; i < length_; i++)
      v_[i] = v1(i).vi_;
  }
};

/**
 * Returns the sum of the coefficients of the specified
 * matrix, column vector or row vector.
 *
 * @tparam R Row type for matrix.
 * @tparam C Column type for matrix.
 * @param m Specified matrix or vector.
 * @return Sum of coefficients of matrix.
 */
template <int R, int C>
inline var sum(const Eigen::Matrix<var, R, C>& m) {
  if (m.size() == 0)
    return 0.0;
  return var(new sum_eigen_v_vari(m));
}

}  // namespace math
}  // namespace stan
#endif
