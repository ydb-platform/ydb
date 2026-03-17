#ifndef STAN_MATH_REV_MAT_FUNCTOR_ALGEBRA_SYSTEM_HPP
#define STAN_MATH_REV_MAT_FUNCTOR_ALGEBRA_SYSTEM_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/rev/mat/functor/jacobian.hpp>
#include <iostream>
#include <string>
#include <vector>

namespace stan {
namespace math {

/**
 * A functor that allows us to treat either x or y as
 * the independent variable. If x_is_dv = true, than the
 * Jacobian is computed w.r.t x, else it is computed
 * w.r.t y.
 * @tparam F type for algebraic system functor
 * @tparam T0 type for unknowns
 * @tparam T1 type for auxiliary parameters
 * @tparam x_is_iv true if x is the independent variable
 */
template <typename F, typename T0, typename T1, bool x_is_iv>
struct system_functor {
  /** algebraic system functor */
  F f_;
  /** unknowns */
  Eigen::Matrix<T0, Eigen::Dynamic, 1> x_;
  /** auxiliary parameters */
  Eigen::Matrix<T1, Eigen::Dynamic, 1> y_;
  /** real data */
  std::vector<double> dat_;
  /** integer data */
  std::vector<int> dat_int_;
  /** stream message */
  std::ostream* msgs_;

  system_functor() {}

  system_functor(const F& f, const Eigen::Matrix<T0, Eigen::Dynamic, 1>& x,
                 const Eigen::Matrix<T1, Eigen::Dynamic, 1>& y,
                 const std::vector<double>& dat,
                 const std::vector<int>& dat_int, std::ostream* msgs)
      : f_(f), x_(x), y_(y), dat_(dat), dat_int_(dat_int), msgs_(msgs) {}

  /**
   * An operator that takes in an independent variable. The
   * independent variable is either passed as the unknown x,
   * or the auxiliary parameter y. The x_is_iv template parameter
   * allows us to determine whether the jacobian is computed
   * with respect to x or y.
   * @tparam T the scalar type of the independent variable
   */
  template <typename T>
  inline Eigen::Matrix<T, Eigen::Dynamic, 1> operator()(
      const Eigen::Matrix<T, Eigen::Dynamic, 1>& iv) const {
    if (x_is_iv)
      return f_(iv, y_, dat_, dat_int_, msgs_);
    else
      return f_(x_, iv, dat_, dat_int_, msgs_);
  }
};

/**
 * A structure which gets passed to Eigen's dogleg
 * algebraic solver.
 * @tparam T scalar type of independent variable.
 * @tparam NX number of rows
 * @tparam NY number of columns
 */
template <typename T, int NX = Eigen::Dynamic, int NY = Eigen::Dynamic>
struct nlo_functor {
  const int m_inputs, m_values;

  nlo_functor() : m_inputs(NX), m_values(NY) {}

  nlo_functor(int inputs, int values) : m_inputs(inputs), m_values(values) {}

  int inputs() const { return m_inputs; }
  int values() const { return m_values; }
};

/**
 * A functor with the required operators to call Eigen's
 * algebraic solver.
 * @tparam S wrapper around the algebraic system functor. Has the
 * signature required for jacobian (i.e takes only one argument).
 * @tparam F algebraic system functor
 * @tparam T0 scalar type for unknowns
 * @tparam T1 scalar type for auxiliary parameters
 */
template <typename S, typename F, typename T0, typename T1>
struct hybrj_functor_solver : nlo_functor<double> {
  /** Wrapper around algebraic system */
  S fs_;
  /** number of unknowns */
  int x_size_;
  /** Jacobian of algebraic function wrt unknowns */
  Eigen::MatrixXd J_;

  hybrj_functor_solver(const S& fs, const F& f,
                       const Eigen::Matrix<T0, Eigen::Dynamic, 1>& x,
                       const Eigen::Matrix<T1, Eigen::Dynamic, 1>& y,
                       const std::vector<double>& dat,
                       const std::vector<int>& dat_int, std::ostream* msgs)
      : fs_(f, x, y, dat, dat_int, msgs), x_size_(x.size()) {}

  /**
   * Computes the value the algebraic function, f, when pluging in the
   * independent variables, and the Jacobian w.r.t unknowns. Required
   * by Eigen.
   * @param [in] iv independent variables
   * @param [in, out] fvec value of algebraic function when plugging in iv.
   */
  int operator()(const Eigen::VectorXd& iv, Eigen::VectorXd& fvec) {
    jacobian(fs_, iv, fvec, J_);
    return 0;
  }

  /**
   * Assign the Jacobian to fjac (signature required by Eigen). Required
   * by Eigen.
   * @param [in] iv independent variables.
   * @param [in, out] fjac matrix container for jacobian
   */
  int df(const Eigen::VectorXd& iv, Eigen::MatrixXd& fjac) const {
    fjac = J_;
    return 0;
  }

  /**
   * Performs the same task as the operator(), but returns the
   * Jacobian, instead of saving it inside an argument
   * passed by reference.
   * @param [in] iv indepdent variable.
   */
  Eigen::MatrixXd get_jacobian(const Eigen::VectorXd& iv) {
    Eigen::VectorXd fvec;
    jacobian(fs_, iv, fvec, J_);
    return J_;
  }

  /**
   * Performs the same task as df(), but returns the value of
   * algebraic function, instead of saving it inside an
   * argument passed by reference.
   * @tparam [in] iv independent variable.
   */
  Eigen::VectorXd get_value(const Eigen::VectorXd& iv) const { return fs_(iv); }
};

}  // namespace math
}  // namespace stan

#endif
