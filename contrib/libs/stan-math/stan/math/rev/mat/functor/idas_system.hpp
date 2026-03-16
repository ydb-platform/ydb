#ifndef STAN_MATH_REV_MAT_FUNCTOR_IDAS_RESIDUAL_HPP
#define STAN_MATH_REV_MAT_FUNCTOR_IDAS_RESIDUAL_HPP

#include <stan/math/prim/arr/fun/value_of.hpp>
#include <stan/math/prim/arr/fun/dot_self.hpp>
#include <stan/math/prim/scal/err/check_greater_or_equal.hpp>
#include <stan/math/prim/scal/err/check_less_or_equal.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/arr/err/check_nonzero_size.hpp>
#include <stan/math/rev/scal/meta/is_var.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/rev/mat/fun/typedefs.hpp>
#include <idas/idas.h>
#include <nvector/nvector_serial.h>
#include <ostream>
#include <vector>

#define CHECK_IDAS_CALL(call) idas_check(call, #call)

/**
 * check IDAS return flag & throw runtime error
 *
 * @param[in] flag routine return flag
 * @param[in] func routine name
 */
inline void idas_check(int flag, const char* func) {
  if (flag < 0) {
    std::ostringstream ss;
    ss << func << " failed with error flag " << flag;
    throw std::runtime_error(ss.str());
  }
}

/**
 * copy NV_Vector* array to Eigen::MatrixXd
 *
 * @param[in] nv N_Vector* array.
 * @param[in] nv_size length of nv.
 * @return Eigen::MatrixXd.
 */
inline Eigen::MatrixXd matrix_d_from_NVarray(const N_Vector* nv,
                                             const size_t& nv_size) {
  size_t m = nv_size;
  size_t n = NV_LENGTH_S(nv[0]);
  stan::math::matrix_d res(n, m);
  for (size_t j = 0; j < m; ++j) {
    auto nvp = N_VGetArrayPointer(nv[j]);
    for (size_t i = 0; i < n; ++i) {
      res(i, j) = nvp[i];
    }
  }
  return res;
}

/**
 * copy Eigen::MatrixXd to NV_Vector* array.
 *
 * @param[in] mat Eigen::MatrixXd to be converted
 * @param[out] nv N_Vector* array
 * @param[in] nv_size length of nv
 */
inline void matrix_d_to_NVarray(const Eigen::MatrixXd& mat, N_Vector* nv,
                                const size_t& nv_size) {
  size_t m = nv_size;
  size_t n = NV_LENGTH_S(nv[0]);
  for (size_t j = 0; j < m; ++j) {
    auto nvp = N_VGetArrayPointer(nv[j]);
    for (size_t i = 0; i < n; ++i) {
      nvp[i] = mat(i, j);
    }
  }
}

namespace stan {
namespace math {

/**
 * IDAS DAE system that contains informtion on residual
 * equation functor, sensitivity residual equation functor,
 * as well as initial conditions. This is a base type that
 * is intended to contain common values used by forward
 * sensitivity system.
 *
 * @tparam F type of functor for DAE residual
 * @tparam Tyy scalar type of initial unknown values
 * @tparam Typ scalar type of initial unknown's derivative values
 * @tparam Tpar scalar type of parameters
 */
template <typename F, typename Tyy, typename Typ, typename Tpar>
class idas_system {
 protected:
  const F& f_;
  const std::vector<Tyy>& yy_;
  const std::vector<Typ>& yp_;
  std::vector<double> yy_val_;  // workspace
  std::vector<double> yp_val_;  // workspace
  const std::vector<Tpar>& theta_;
  const std::vector<double>& x_r_;
  const std::vector<int>& x_i_;
  const size_t N_;
  const size_t M_;
  const size_t ns_;  // nb. of sensi params
  N_Vector nv_yy_;
  N_Vector nv_yp_;
  std::vector<double> rr_val_;  // workspace
  N_Vector nv_rr_;
  N_Vector id_;
  void* mem_;
  std::ostream* msgs_;

 public:
  static constexpr bool is_var_yy0 = stan::is_var<Tyy>::value;
  static constexpr bool is_var_yp0 = stan::is_var<Typ>::value;
  static constexpr bool is_var_par = stan::is_var<Tpar>::value;
  static constexpr bool need_sens = is_var_yy0 || is_var_yp0 || is_var_par;

  using scalar_type = typename stan::return_type<Tyy, Typ, Tpar>::type;
  using return_type = std::vector<std::vector<scalar_type> >;

  /**
   * Construct IDAS DAE system from initial condition and parameters
   *
   * @param[in] f DAE residual functor
   * @param[in] eq_id array for DAE's variable ID(1 for *
   *                  derivative variables, 0 for algebraic variables).
   * @param[in] yy0 initial condiiton
   * @param[in] yp0 initial condiiton for derivatives
   * @param[in] theta parameters of the base DAE.
   * @param[in] x_r continuous data vector for the DAE.
   * @param[in] x_i integer data vector for the DAE.
   * @param[in] msgs stream to which messages are printed.
   */
  idas_system(const F& f, const std::vector<int>& eq_id,
              const std::vector<Tyy>& yy0, const std::vector<Typ>& yp0,
              const std::vector<Tpar>& theta, const std::vector<double>& x_r,
              const std::vector<int>& x_i, std::ostream* msgs)
      : f_(f),
        yy_(yy0),
        yp_(yp0),
        yy_val_(value_of(yy0)),
        yp_val_(value_of(yp0)),
        theta_(theta),
        x_r_(x_r),
        x_i_(x_i),
        N_(yy0.size()),
        M_(theta.size()),
        ns_((is_var_yy0 ? N_ : 0) + (is_var_yp0 ? N_ : 0)
            + (is_var_par ? M_ : 0)),
        nv_yy_(N_VMake_Serial(N_, yy_val_.data())),
        nv_yp_(N_VMake_Serial(N_, yp_val_.data())),
        rr_val_(N_, 0.0),
        nv_rr_(N_VMake_Serial(N_, rr_val_.data())),
        id_(N_VNew_Serial(N_)),
        mem_(IDACreate()),
        msgs_(msgs) {
    if (nv_yy_ == NULL || nv_yp_ == NULL)
      throw std::runtime_error("N_VMake_Serial failed to allocate memory");

    if (mem_ == NULL)
      throw std::runtime_error("IDACreate failed to allocate memory");

    static const char* caller = "idas_system";
    check_finite(caller, "initial state", yy0);
    check_finite(caller, "derivative initial state", yp0);
    check_finite(caller, "parameter vector", theta);
    check_finite(caller, "continuous data", x_r);
    check_nonzero_size(caller, "initial state", yy0);
    check_nonzero_size(caller, "derivative initial state", yp0);
    check_consistent_sizes(caller, "initial state", yy0,
                           "derivative initial state", yp0);
    check_consistent_sizes(caller, "initial state", yy0,
                           "derivative-algebra id", eq_id);
    check_greater_or_equal(caller, "derivative-algebra id", eq_id, 0);
    check_less_or_equal(caller, "derivative-algebra id", eq_id, 1);

    for (size_t i = 0; i < N_; ++i)
      NV_Ith_S(id_, i) = eq_id[i];
  }

  /**
   * destructor to deallocate IDAS solution memory and workspace.
   */
  ~idas_system() {
    N_VDestroy_Serial(nv_yy_);
    N_VDestroy_Serial(nv_yp_);
    N_VDestroy_Serial(nv_rr_);
    N_VDestroy_Serial(id_);
    IDAFree(&mem_);
  }

  /**
   * return reference to current N_Vector of unknown variable
   *
   * @return reference to current N_Vector of unknown variable
   */
  N_Vector& nv_yy() { return nv_yy_; }

  /**
   * return reference to current N_Vector of derivative variable
   *
   * @return reference to current N_Vector of derivative variable
   */
  N_Vector& nv_yp() { return nv_yp_; }

  /**
   * return reference to current N_Vector of residual workspace
   *
   * @return reference to current N_Vector of residual workspace
   */
  N_Vector& nv_rr() { return nv_rr_; }

  /**
   * return reference to DAE variable IDs
   *
   * @return reference to DAE variable IDs.
   */
  N_Vector& id() { return id_; }

  /**
   * return reference to current solution vector value
   *
   * @return reference to current solution vector value
   */
  const std::vector<double>& yy_val() { return yy_val_; }

  /**
   * return reference to current solution derivative vector value
   *
   * @return reference to current solution derivative vector value
   */
  const std::vector<double>& yp_val() { return yp_val_; }

  /**
   * return reference to initial condition
   *
   * @return reference to initial condition
   */
  const std::vector<Tyy>& yy0() const { return yy_; }

  /**
   * return reference to derivative initial condition
   *
   * @return reference to derivative initial condition
   */
  const std::vector<Typ>& yp0() const { return yp_; }

  /**
   * return reference to parameter
   *
   * @return reference to parameter
   */
  const std::vector<Tpar>& theta() const { return theta_; }

  /**
   * return a vector of vars for that contains the initial
   * condition and parameters in case they are vars. The
   * sensitivity with respect to this vector will be
   * calculated by IDAS.
   *
   * @return vector of vars
   */
  std::vector<scalar_type> vars() const {
    std::vector<scalar_type> res;
    if (is_var_yy0) {
      res.insert(res.end(), yy0().begin(), yy0().end());
    }
    if (is_var_yp0) {
      res.insert(res.end(), yp0().begin(), yp0().end());
    }
    if (is_var_par) {
      res.insert(res.end(), theta().begin(), theta().end());
    }

    return res;
  }

  /**
   * return number of unknown variables
   */
  const size_t n() { return N_; }

  /**
   * return number of sensitivity parameters
   */
  const size_t ns() { return ns_; }

  /**
   * return size of DAE system for primary and sensitivity unknowns
   */
  const size_t n_sys() { return N_ * (ns_ + 1); }

  /**
   * return theta size
   */
  const size_t n_par() { return theta_.size(); }

  /**
   * return IDAS memory handle
   */
  void* mem() { return mem_; }

  /**
   * return reference to DAE functor
   */
  const F& f() { return f_; }

  /**
   * return a closure for IDAS residual callback
   */
  IDAResFn residual() {  // a non-capture lambda
    return [](double t, N_Vector yy, N_Vector yp, N_Vector rr,
              void* user_data) -> int {
      using DAE = idas_system<F, Tyy, Typ, Tpar>;
      DAE* dae = static_cast<DAE*>(user_data);

      size_t N = NV_LENGTH_S(yy);
      auto yy_val = N_VGetArrayPointer(yy);
      std::vector<double> yy_vec(yy_val, yy_val + N);
      auto yp_val = N_VGetArrayPointer(yp);
      std::vector<double> yp_vec(yp_val, yp_val + N);
      auto res = dae->f_(t, yy_vec, yp_vec, dae->theta_, dae->x_r_, dae->x_i_,
                         dae->msgs_);
      for (size_t i = 0; i < N; ++i)
        NV_Ith_S(rr, i) = value_of(res[i]);

      return 0;
    };
  }

  void check_ic_consistency(const double& t0, const double& tol) {
    const std::vector<double> theta_d(value_of(theta_));
    const std::vector<double> yy_d(value_of(yy_));
    const std::vector<double> yp_d(value_of(yp_));
    static const char* caller = "idas_integrator";
    std::vector<double> res(f_(t0, yy_d, yp_d, theta_d, x_r_, x_i_, msgs_));
    double res0 = std::sqrt(dot_self(res));
    check_less_or_equal(caller, "DAE residual at t0", res0, tol);
  }
};

// TODO(yizhang): adjoint system construction

}  // namespace math
}  // namespace stan

#endif
