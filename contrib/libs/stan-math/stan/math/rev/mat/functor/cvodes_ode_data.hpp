#ifndef STAN_MATH_REV_MAT_FUNCTOR_CVODES_ODE_DATA_HPP
#define STAN_MATH_REV_MAT_FUNCTOR_CVODES_ODE_DATA_HPP

#include <stan/math/prim/arr/functor/coupled_ode_system.hpp>
#include <stan/math/rev/scal/meta/is_var.hpp>
#include <stan/math/rev/arr/functor/coupled_ode_system.hpp>
#include <cvodes/cvodes.h>
#include <sunmatrix/sunmatrix_dense.h>
#include <sunlinsol/sunlinsol_dense.h>
#include <nvector/nvector_serial.h>
#include <algorithm>
#include <vector>

namespace stan {
namespace math {

/**
 * CVODES ode data holder object which is used during CVODES
 * integration for CVODES callbacks.
 *
 * @tparam F type of functor for the base ode system.
 * @tparam T_initial type of initial values
 * @tparam T_param type of parameters
 */

template <typename F, typename T_initial, typename T_param>
class cvodes_ode_data {
  const F& f_;
  const std::vector<T_initial>& y0_;
  const std::vector<T_param>& theta_;
  const std::vector<double> theta_dbl_;
  const size_t N_;
  const size_t M_;
  const std::vector<double>& x_;
  const std::vector<int>& x_int_;
  std::ostream* msgs_;
  const size_t S_;

  typedef cvodes_ode_data<F, T_initial, T_param> ode_data;
  typedef stan::is_var<T_initial> initial_var;
  typedef stan::is_var<T_param> param_var;

 public:
  const coupled_ode_system<F, T_initial, T_param> coupled_ode_;
  std::vector<double> coupled_state_;
  N_Vector nv_state_;
  N_Vector* nv_state_sens_;
  SUNMatrix A_;
  SUNLinearSolver LS_;

  /**
   * Construct CVODES ode data object to enable callbacks from
   * CVODES during ODE integration. Static callbacks are defined
   * for the ODE RHS (<code>cv_rhs</code>), the ODE sensitivity
   * RHS (<code>cv_rhs_sens</code>) and for the ODE Jacobian wrt
   * to the states (<code>cv_jacobian_states</code>).
   *
   * The callbacks required by CVODES are detailled in
   * https://computation.llnl.gov/sites/default/files/public/cvs_guide.pdf
   *
   * Note: The supplied callbacks do always return 0 which flags to
   * CVODES that the function was successfully evaluated. Errors are
   * handled within Stan using exceptions such that any thrown error
   * leads to the termination of the ODE integration.
   *
   * @param[in] f ode functor.
   * @param[in] y0 initial state of the base ode.
   * @param[in] theta parameters of the base ode.
   * @param[in] x continuous data vector for the ODE.
   * @param[in] x_int integer data vector for the ODE.
   * @param[in] msgs stream to which messages are printed.
   */
  cvodes_ode_data(const F& f, const std::vector<T_initial>& y0,
                  const std::vector<T_param>& theta,
                  const std::vector<double>& x, const std::vector<int>& x_int,
                  std::ostream* msgs)
      : f_(f),
        y0_(y0),
        theta_(theta),
        theta_dbl_(value_of(theta)),
        N_(y0.size()),
        M_(theta.size()),
        x_(x),
        x_int_(x_int),
        msgs_(msgs),
        S_((initial_var::value ? N_ : 0) + (param_var::value ? M_ : 0)),
        coupled_ode_(f, y0, theta, x, x_int, msgs),
        coupled_state_(coupled_ode_.initial_state()),
        nv_state_(N_VMake_Serial(N_, &coupled_state_[0])),
        nv_state_sens_(nullptr),
        A_(SUNDenseMatrix(N_, N_)),
        LS_(SUNDenseLinearSolver(nv_state_, A_)) {
    if (S_ > 0) {
      nv_state_sens_ = N_VCloneVectorArrayEmpty_Serial(S_, nv_state_);
      for (std::size_t i = 0; i < S_; i++) {
        NV_DATA_S(nv_state_sens_[i]) = &coupled_state_[N_] + i * N_;
      }
    }
  }

  ~cvodes_ode_data() {
    SUNLinSolFree(LS_);
    SUNMatDestroy(A_);
    N_VDestroy_Serial(nv_state_);
    if (S_ > 0)
      N_VDestroyVectorArray_Serial(nv_state_sens_, S_);
  }

  /**
   * Implements the function of type CVRhsFn which is the user-defined
   * ODE RHS passed to CVODES.
   */
  static int cv_rhs(realtype t, N_Vector y, N_Vector ydot, void* user_data) {
    const ode_data* explicit_ode = static_cast<const ode_data*>(user_data);
    explicit_ode->rhs(t, NV_DATA_S(y), NV_DATA_S(ydot));
    return 0;
  }

  /**
   * Implements the function of type CVSensRhsFn which is the
   * RHS of the sensitivity ODE system.
   */
  static int cv_rhs_sens(int Ns, realtype t, N_Vector y, N_Vector ydot,
                         N_Vector* yS, N_Vector* ySdot, void* user_data,
                         N_Vector tmp1, N_Vector tmp2) {
    const ode_data* explicit_ode = static_cast<const ode_data*>(user_data);
    explicit_ode->rhs_sens(t, NV_DATA_S(y), yS, ySdot);
    return 0;
  }

  /**
   * Implements the function of type CVDlsJacFn which is the
   * user-defined callback for CVODES to calculate the jacobian of the
   * ode_rhs wrt to the states y. The jacobian is stored in column
   * major format.
   */
  static int cv_jacobian_states(realtype t, N_Vector y, N_Vector fy,
                                SUNMatrix J, void* user_data, N_Vector tmp1,
                                N_Vector tmp2, N_Vector tmp3) {
    const ode_data* explicit_ode = static_cast<const ode_data*>(user_data);
    return explicit_ode->jacobian_states(t, NV_DATA_S(y), J);
  }

 private:
  /**
   * Calculates the ODE RHS, dy_dt, using the user-supplied functor at
   * the given time t and state y.
   */
  inline void rhs(double t, const double y[], double dy_dt[]) const {
    const std::vector<double> y_vec(y, y + N_);
    const std::vector<double> dy_dt_vec
        = f_(t, y_vec, theta_dbl_, x_, x_int_, msgs_);
    check_size_match("cvodes_ode_data", "dz_dt", dy_dt_vec.size(), "states",
                     N_);
    std::copy(dy_dt_vec.begin(), dy_dt_vec.end(), dy_dt);
  }

  /**
   * Calculates the jacobian of the ODE RHS wrt to its states y at the
   * given time-point t and state y.
   * Note that the jacobian of the ODE system is the coupled ode system for
   * varying states evaluated at the state y whenever we choose state
   * y to be the initial of the coupled ode system.
   */
  inline int jacobian_states(double t, const double y[], SUNMatrix J) const {
    const std::vector<double> y_vec(y, y + N_);
    start_nested();
    std::vector<var> y_vec_var(y_vec.begin(), y_vec.end());
    coupled_ode_system<F, var, double> ode_jacobian(f_, y_vec_var, theta_dbl_,
                                                    x_, x_int_, msgs_);
    std::vector<double> jacobian_y(ode_jacobian.size(), 0);
    ode_jacobian(ode_jacobian.initial_state(), jacobian_y, t);
    std::copy(jacobian_y.begin() + N_, jacobian_y.end(), SM_DATA_D(J));
    recover_memory_nested();
    return 0;
  }

  /**
   * Calculates the RHS of the sensitivity ODE system which
   * corresponds to the coupled ode system from which the first N
   * states are omitted, since the first N states are the ODE RHS
   * which CVODES separates from the main ODE RHS.
   */
  inline void rhs_sens(double t, const double y[], N_Vector* yS,
                       N_Vector* ySdot) const {
    std::vector<double> z(coupled_state_.size());
    std::vector<double> dz_dt(coupled_state_.size());
    std::copy(y, y + N_, z.begin());
    for (std::size_t s = 0; s < S_; s++)
      std::copy(NV_DATA_S(yS[s]), NV_DATA_S(yS[s]) + N_,
                z.begin() + (s + 1) * N_);
    coupled_ode_(z, dz_dt, t);
    for (std::size_t s = 0; s < S_; s++)
      std::copy(dz_dt.begin() + (s + 1) * N_, dz_dt.begin() + (s + 2) * N_,
                NV_DATA_S(ySdot[s]));
  }
};

}  // namespace math
}  // namespace stan
#endif
