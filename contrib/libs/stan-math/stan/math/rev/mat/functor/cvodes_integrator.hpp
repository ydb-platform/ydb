#ifndef STAN_MATH_REV_MAT_FUNCTOR_INTEGRATE_ODE_CVODES_HPP
#define STAN_MATH_REV_MAT_FUNCTOR_INTEGRATE_ODE_CVODES_HPP

#include <stan/math/prim/arr/fun/value_of.hpp>
#include <stan/math/prim/scal/err/check_less.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/arr/err/check_nonzero_size.hpp>
#include <stan/math/prim/arr/err/check_ordered.hpp>
#include <stan/math/prim/arr/functor/coupled_ode_system.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/rev/scal/meta/is_var.hpp>
#include <stan/math/rev/arr/functor/coupled_ode_system.hpp>
#include <stan/math/rev/mat/functor/cvodes_utils.hpp>
#include <stan/math/rev/mat/functor/cvodes_ode_data.hpp>
#include <cvodes/cvodes.h>
#include <sunlinsol/sunlinsol_dense.h>
#include <algorithm>
#include <ostream>
#include <vector>

namespace stan {
namespace math {

/**
 * Integrator interface for CVODES' ODE solvers (Adams & BDF
 * methods).
 * @tparam Lmm ID of ODE solver (1: ADAMS, 2: BDF)
 */
template <int Lmm>
class cvodes_integrator {
 public:
  cvodes_integrator() {}

  /**
   * Return the solutions for the specified system of ordinary
   * differential equations given the specified initial state,
   * initial times, times of desired solution, and parameters and
   * data, writing error and warning messages to the specified
   * stream.
   *
   * This function is templated to allow the initials to be
   * either data or autodiff variables and the parameters to be data
   * or autodiff variables.  The autodiff-based implementation for
   * reverse-mode are defined in namespace <code>stan::math</code>
   * and may be invoked via argument-dependent lookup by including
   * their headers.
   *
   * The solver used is based on the backward differentiation
   * formula which is an implicit numerical integration scheme
   * appropiate for stiff ODE systems.
   *
   * @tparam F type of ODE system function.
   * @tparam T_initial type of scalars for initial values.
   * @tparam T_param type of scalars for parameters.
   * @param[in] f functor for the base ordinary differential equation.
   * @param[in] y0 initial state.
   * @param[in] t0 initial time.
   * @param[in] ts times of the desired solutions, in strictly
   * increasing order, all greater than the initial time.
   * @param[in] theta parameter vector for the ODE.
   * @param[in] x continuous data vector for the ODE.
   * @param[in] x_int integer data vector for the ODE.
   * @param[in, out] msgs the print stream for warning messages.
   * @param[in] relative_tolerance relative tolerance passed to CVODE.
   * @param[in] absolute_tolerance absolute tolerance passed to CVODE.
   * @param[in] max_num_steps maximal number of admissable steps
   * between time-points
   * @return a vector of states, each state being a vector of the
   * same size as the state variable, corresponding to a time in ts.
   */
  template <typename F, typename T_initial, typename T_param>
  std::vector<
      std::vector<typename stan::return_type<T_initial, T_param>::type> >
  integrate(const F& f, const std::vector<T_initial>& y0, double t0,
            const std::vector<double>& ts, const std::vector<T_param>& theta,
            const std::vector<double>& x, const std::vector<int>& x_int,
            std::ostream* msgs, double relative_tolerance,
            double absolute_tolerance,
            long int max_num_steps) {  // NOLINT(runtime/int)
    typedef stan::is_var<T_initial> initial_var;
    typedef stan::is_var<T_param> param_var;

    const char* fun = "integrate_ode_cvodes";

    check_finite(fun, "initial state", y0);
    check_finite(fun, "initial time", t0);
    check_finite(fun, "times", ts);
    check_finite(fun, "parameter vector", theta);
    check_finite(fun, "continuous data", x);
    check_nonzero_size(fun, "times", ts);
    check_nonzero_size(fun, "initial state", y0);
    check_ordered(fun, "times", ts);
    check_less(fun, "initial time", t0, ts[0]);
    if (relative_tolerance <= 0)
      invalid_argument("integrate_ode_cvodes", "relative_tolerance,",
                       relative_tolerance, "", ", must be greater than 0");
    if (absolute_tolerance <= 0)
      invalid_argument("integrate_ode_cvodes", "absolute_tolerance,",
                       absolute_tolerance, "", ", must be greater than 0");
    if (max_num_steps <= 0)
      invalid_argument("integrate_ode_cvodes", "max_num_steps,", max_num_steps,
                       "", ", must be greater than 0");

    const size_t N = y0.size();
    const size_t M = theta.size();
    const size_t S = (initial_var::value ? N : 0) + (param_var::value ? M : 0);

    typedef cvodes_ode_data<F, T_initial, T_param> ode_data;
    ode_data cvodes_data(f, y0, theta, x, x_int, msgs);

    void* cvodes_mem = CVodeCreate(Lmm);
    if (cvodes_mem == nullptr)
      throw std::runtime_error("CVodeCreate failed to allocate memory");

    const size_t coupled_size = cvodes_data.coupled_ode_.size();

    std::vector<std::vector<double> > y_coupled(
        ts.size(), std::vector<double>(coupled_size, 0));

    try {
      cvodes_check_flag(
          CVodeInit(cvodes_mem, &ode_data::cv_rhs, t0, cvodes_data.nv_state_),
          "CVodeInit");

      // Assign pointer to this as user data
      cvodes_check_flag(
          CVodeSetUserData(cvodes_mem, reinterpret_cast<void*>(&cvodes_data)),
          "CVodeSetUserData");

      cvodes_set_options(cvodes_mem, relative_tolerance, absolute_tolerance,
                         max_num_steps);

      // for the stiff solvers we need to reserve additional memory
      // and provide a Jacobian function call. new API since 3.0.0:
      // create matrix object and linear solver object; resource
      // (de-)allocation is handled in the cvodes_ode_data
      cvodes_check_flag(
          CVodeSetLinearSolver(cvodes_mem, cvodes_data.LS_, cvodes_data.A_),
          "CVodeSetLinearSolver");
      cvodes_check_flag(
          CVodeSetJacFn(cvodes_mem, &ode_data::cv_jacobian_states),
          "CVodeSetJacFn");

      // initialize forward sensitivity system of CVODES as needed
      if (S > 0) {
        cvodes_check_flag(
            CVodeSensInit(cvodes_mem, static_cast<int>(S), CV_STAGGERED,
                          &ode_data::cv_rhs_sens, cvodes_data.nv_state_sens_),
            "CVodeSensInit");

        cvodes_check_flag(CVodeSensEEtolerances(cvodes_mem),
                          "CVodeSensEEtolerances");
      }

      double t_init = t0;
      for (size_t n = 0; n < ts.size(); ++n) {
        double t_final = ts[n];
        if (t_final != t_init)
          cvodes_check_flag(CVode(cvodes_mem, t_final, cvodes_data.nv_state_,
                                  &t_init, CV_NORMAL),
                            "CVode");
        if (S > 0) {
          cvodes_check_flag(
              CVodeGetSens(cvodes_mem, &t_init, cvodes_data.nv_state_sens_),
              "CVodeGetSens");
        }
        std::copy(cvodes_data.coupled_state_.begin(),
                  cvodes_data.coupled_state_.end(), y_coupled[n].begin());
        t_init = t_final;
      }
    } catch (const std::exception& e) {
      CVodeFree(&cvodes_mem);
      throw;
    }

    CVodeFree(&cvodes_mem);

    return cvodes_data.coupled_ode_.decouple_states(y_coupled);
  }
};  // cvodes integrator
}  // namespace math
}  // namespace stan
#endif
