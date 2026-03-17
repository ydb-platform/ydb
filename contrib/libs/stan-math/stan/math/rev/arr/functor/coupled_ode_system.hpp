#ifndef STAN_MATH_REV_ARR_FUNCTOR_COUPLED_ODE_SYSTEM_HPP
#define STAN_MATH_REV_ARR_FUNCTOR_COUPLED_ODE_SYSTEM_HPP

#include <stan/math/prim/arr/meta/get.hpp>
#include <stan/math/prim/arr/meta/length.hpp>
#include <stan/math/prim/arr/functor/coupled_ode_system.hpp>
#include <stan/math/prim/arr/fun/value_of.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <stan/math/rev/scal/fun/value_of_rec.hpp>
#include <stan/math/rev/core.hpp>
#include <ostream>
#include <stdexcept>
#include <vector>

namespace stan {
namespace math {

// This code is in this directory because it includes var
// It is in namespace stan::math so that the partial template
// specializations are treated as such.

/**
 * The coupled ODE system for known initial values and unknown
 * parameters.
 *
 * <p>If the base ODE state is size N and there are M parameters,
 * the coupled system has N + N * M states.
 * <p>The first N states correspond to the base system's N states:
 * \f$ \frac{d x_n}{dt} \f$
 *
 * <p>The next M states correspond to the sensitivities of the
 * parameters with respect to the first base system equation:
 * \f[
 *   \frac{d x_{N+m}}{dt}
 *   = \frac{d}{dt} \frac{\partial x_1}{\partial \theta_m}
 * \f]
 *
 * <p>The final M states correspond to the sensitivities with respect
 * to the second base system equation, etc.
 *
 * @tparam F type of functor for the base ode system.
 */
template <typename F>
struct coupled_ode_system<F, double, var> {
  const F& f_;
  const std::vector<double>& y0_dbl_;
  const std::vector<var>& theta_;
  const std::vector<double> theta_dbl_;
  const std::vector<double>& x_;
  const std::vector<int>& x_int_;
  const size_t N_;
  const size_t M_;
  const size_t size_;
  std::ostream* msgs_;

  /**
   * Construct a coupled ODE system with the specified base
   * ODE system, base initial state, parameters, data, and a
   * message stream.
   *
   * @param[in] f the base ODE system functor.
   * @param[in] y0 the initial state of the base ode.
   * @param[in] theta parameters of the base ode.
   * @param[in] x real data.
   * @param[in] x_int integer data.
   * @param[in, out] msgs stream to which messages are printed.
   */
  coupled_ode_system(const F& f, const std::vector<double>& y0,
                     const std::vector<var>& theta,
                     const std::vector<double>& x,
                     const std::vector<int>& x_int, std::ostream* msgs)
      : f_(f),
        y0_dbl_(y0),
        theta_(theta),
        theta_dbl_(value_of(theta)),
        x_(x),
        x_int_(x_int),
        N_(y0.size()),
        M_(theta.size()),
        size_(N_ + N_ * M_),
        msgs_(msgs) {}

  /**
   * Assign the derivative vector with the system derivatives at
   * the specified state and time.
   *
   * <p>The input state must be of size <code>size()</code>, and
   * the output produced will be of the same size.
   *
   * @param[in] z state of the coupled ode system.
   * @param[out] dz_dt populated with the derivatives of
   * the coupled system at the specified state and time.
   * @param[in]  t time.
   * @throw exception if the system function does not return the
   * same number of derivatives as the state vector size.
   *
   * y is the base ODE system state
   *
   */
  void operator()(const std::vector<double>& z, std::vector<double>& dz_dt,
                  double t) const {
    using std::vector;

    try {
      start_nested();

      vector<var> y_vars(z.begin(), z.begin() + N_);

      vector<var> theta_vars(theta_dbl_.begin(), theta_dbl_.end());

      vector<var> dy_dt_vars = f_(t, y_vars, theta_vars, x_, x_int_, msgs_);

      check_size_match("coupled_ode_system", "dz_dt", dy_dt_vars.size(),
                       "states", N_);

      for (size_t i = 0; i < N_; i++) {
        dz_dt[i] = dy_dt_vars[i].val();
        dy_dt_vars[i].grad();

        for (size_t j = 0; j < M_; j++) {
          // orders derivatives by equation (i.e. if there are 2 eqns
          // (y1, y2) and 2 parameters (a, b), dy_dt will be ordered as:
          // dy1_dt, dy2_dt, dy1_da, dy2_da, dy1_db, dy2_db
          double temp_deriv = theta_vars[j].adj();
          const size_t offset = N_ + N_ * j;
          for (size_t k = 0; k < N_; k++)
            temp_deriv += z[offset + k] * y_vars[k].adj();

          dz_dt[offset + i] = temp_deriv;
        }

        set_zero_all_adjoints_nested();
      }
    } catch (const std::exception& e) {
      recover_memory_nested();
      throw;
    }
    recover_memory_nested();
  }

  /**
   * Returns the size of the coupled system.
   *
   * @return size of the coupled system.
   */
  size_t size() const { return size_; }

  /**
   * Returns the initial state of the coupled system.  Because the
   * initial values are known, the initial state of the coupled
   * system is the same as the initial state of the base ODE
   * system.
   *
   * <p>This initial state returned is of size <code>size()</code>
   * where the first N (base ODE system size) parameters are the
   * initial conditions of the base ode system and the rest of the
   * initial condition elements are 0.
   *
   * @return the initial condition of the coupled system.
   */
  std::vector<double> initial_state() const {
    std::vector<double> state(size_, 0.0);
    for (size_t n = 0; n < N_; n++)
      state[n] = y0_dbl_[n];
    return state;
  }

  /**
   * Returns the base ODE system state corresponding to the
   * specified coupled system state.
   *
   * @param y coupled states after solving the ode
   */
  std::vector<std::vector<var> > decouple_states(
      const std::vector<std::vector<double> >& y) const {
    std::vector<var> temp_vars(N_);
    std::vector<double> temp_gradients(M_);
    std::vector<std::vector<var> > y_return(y.size());

    for (size_t i = 0; i < y.size(); i++) {
      // iterate over number of equations
      for (size_t j = 0; j < N_; j++) {
        // iterate over parameters for each equation
        for (size_t k = 0; k < M_; k++)
          temp_gradients[k] = y[i][y0_dbl_.size() + y0_dbl_.size() * k + j];

        temp_vars[j] = precomputed_gradients(y[i][j], theta_, temp_gradients);
      }
      y_return[i] = temp_vars;
    }
    return y_return;
  }
};

/**
 * The coupled ODE system for unknown initial values and known
 * parameters.
 *
 * <p>If the original ODE has states of size N, the
 * coupled system has N + N * N states. (derivatives of each
 * state with respect to each initial value)
 *
 * <p>The coupled system has N + N * N states, where N is the size of
 * the state vector in the base system.
 *
 * <p>The first N states correspond to the base system's N states:
 * \f$ \frac{d x_n}{dt} \f$
 *
 * <p>The next N states correspond to the sensitivities of the initial
 * conditions with respect to the to the first base system equation:
 * \f[
 *  \frac{d x_{N+n}}{dt}
 *     = \frac{d}{dt} \frac{\partial x_1}{\partial y0_n}
 * \f]
 *
 * <p>The next N states correspond to the sensitivities with respect
 * to the second base system equation, etc.
 *
 * @tparam F type of base ODE system functor
 */
template <typename F>
struct coupled_ode_system<F, var, double> {
  const F& f_;
  const std::vector<var>& y0_;
  const std::vector<double> y0_dbl_;
  const std::vector<double>& theta_dbl_;
  const std::vector<double>& x_;
  const std::vector<int>& x_int_;
  std::ostream* msgs_;
  const size_t N_;
  const size_t M_;
  const size_t size_;

  /**
   * Construct a coupled ODE system for an unknown initial state
   * and known parameters givne the specified base system functor,
   * base initial state, parameters, data, and an output stream
   * for messages.
   *
   * @param[in] f base ODE system functor.
   * @param[in] y0 initial state of the base ODE.
   * @param[in] theta system parameters.
   * @param[in] x real data.
   * @param[in] x_int integer data.
   * @param[in, out] msgs output stream for messages.
   */
  coupled_ode_system(const F& f, const std::vector<var>& y0,
                     const std::vector<double>& theta,
                     const std::vector<double>& x,
                     const std::vector<int>& x_int, std::ostream* msgs)
      : f_(f),
        y0_(y0),
        y0_dbl_(value_of(y0)),
        theta_dbl_(theta),
        x_(x),
        x_int_(x_int),
        msgs_(msgs),
        N_(y0.size()),
        M_(theta.size()),
        size_(N_ + N_ * N_) {}

  /**
   * Calculates the derivative of the coupled ode system
   * with respect to the state y at time t.
   *
   * @param[in] z the current state of the coupled, shifted ode
   * system. This is a a vector of double of length size().
   * @param[out] dz_dt a vector of length size() with the
   * derivatives of the coupled system evaluated with state y and
   * time t.
   * @param[in] t time.
   * @throw exception if the system functor does not return a
   * derivative vector of the same size as the state vector.
   *
   * y is the base ODE system state
   *
   */
  void operator()(const std::vector<double>& z, std::vector<double>& dz_dt,
                  double t) const {
    using std::vector;

    try {
      start_nested();

      vector<var> y_vars(z.begin(), z.begin() + N_);

      vector<var> dy_dt_vars = f_(t, y_vars, theta_dbl_, x_, x_int_, msgs_);

      check_size_match("coupled_ode_system", "dz_dt", dy_dt_vars.size(),
                       "states", N_);

      for (size_t i = 0; i < N_; i++) {
        dz_dt[i] = dy_dt_vars[i].val();
        dy_dt_vars[i].grad();

        for (size_t j = 0; j < N_; j++) {
          // orders derivatives by equation (i.e. if there are 2 eqns
          // (y1, y2) and 2 parameters (a, b), dy_dt will be ordered as:
          // dy1_dt, dy2_dt, dy1_da, dy2_da, dy1_db, dy2_db
          double temp_deriv = 0;
          const size_t offset = N_ + N_ * j;
          for (size_t k = 0; k < N_; k++)
            temp_deriv += z[offset + k] * y_vars[k].adj();

          dz_dt[offset + i] = temp_deriv;
        }

        set_zero_all_adjoints_nested();
      }
    } catch (const std::exception& e) {
      recover_memory_nested();
      throw;
    }
    recover_memory_nested();
  }

  /**
   * Returns the size of the coupled system.
   *
   * @return size of the coupled system.
   */
  size_t size() const { return size_; }

  /**
   * Returns the initial state of the coupled system.
   *
   * <p>Because the starting state is unknown, the coupled system
   * incorporates the initial conditions as parameters.  The
   * initial conditions for the coupled part of the system are set
   * to zero along with the rest of the initial state, because the
   * value of the initial state has been moved into the
   * parameters.
   *
   * @return the initial condition of the coupled system.
   *   This is a vector of length size() where all elements
   *   are 0.
   */
  std::vector<double> initial_state() const {
    std::vector<double> initial(size_, 0.0);
    for (size_t i = 0; i < N_; i++)
      initial[i] = y0_dbl_[i];
    for (size_t i = 0; i < N_; i++)
      initial[N_ + i * N_ + i] = 1.0;
    return initial;
  }

  /**
   * Return the solutions to the basic ODE system, including
   * appropriate autodiff partial derivatives, given the specified
   * coupled system solution.
   *
   * @param y the vector of the coupled states after solving the ode
   */
  std::vector<std::vector<var> > decouple_states(
      const std::vector<std::vector<double> >& y) const {
    using std::vector;

    vector<var> temp_vars(N_);
    vector<double> temp_gradients(N_);
    vector<vector<var> > y_return(y.size());

    for (size_t i = 0; i < y.size(); i++) {
      // iterate over number of equations
      for (size_t j = 0; j < N_; j++) {
        // iterate over parameters for each equation
        for (size_t k = 0; k < N_; k++)
          temp_gradients[k] = y[i][y0_.size() + y0_.size() * k + j];

        temp_vars[j] = precomputed_gradients(y[i][j], y0_, temp_gradients);
      }
      y_return[i] = temp_vars;
    }

    return y_return;
  }
};

/**
 * The coupled ode system for unknown intial values and unknown
 * parameters.
 *
 * <p>The coupled system has N + N * (N + M) states, where N is
 * size of the base ODE state vector and M is the number of
 * parameters.
 *
 * <p>The first N states correspond to the base system's N states:
 *   \f$ \frac{d x_n}{dt} \f$
 *
 * <p>The next N+M states correspond to the sensitivities of the
 * initial conditions, then to the parameters with respect to the
 * to the first base system equation:
 *
 * \f[
 *   \frac{d x_{N + n}}{dt}
 *     = \frac{d}{dt} \frac{\partial x_1}{\partial y0_n}
 * \f]
 *
 * \f[
 *   \frac{d x_{N+N+m}}{dt}
 *     = \frac{d}{dt} \frac{\partial x_1}{\partial \theta_m}
 * \f]
 *
 * <p>The next N+M states correspond to the sensitivities with
 * respect to the second base system equation, etc.
 *
 * <p>If the original ode has a state vector of size N states and
 * a parameter vector of size M, the coupled system has N + N * (N
 * + M) states. (derivatives of each state with respect to each
 * initial value and each theta)
 *
 * @tparam F the functor for the base ode system
 */
template <typename F>
struct coupled_ode_system<F, var, var> {
  const F& f_;
  const std::vector<var>& y0_;
  const std::vector<double> y0_dbl_;
  const std::vector<var>& theta_;
  const std::vector<double> theta_dbl_;
  const std::vector<double>& x_;
  const std::vector<int>& x_int_;
  const size_t N_;
  const size_t M_;
  const size_t size_;
  std::ostream* msgs_;

  /**
   * Construct a coupled ODE system with unknown initial value and
   * known parameters, given the base ODE system functor, the
   * initial state of the base ODE, the parameters, data, and an
   * output stream to which to write messages.
   *
   * @param[in] f the base ode system functor.
   * @param[in] y0 the initial state of the base ode.
   * @param[in] theta parameters of the base ode.
   * @param[in] x real data.
   * @param[in] x_int integer data.
   * @param[in, out] msgs output stream to which to print messages.
   */
  coupled_ode_system(const F& f, const std::vector<var>& y0,
                     const std::vector<var>& theta,
                     const std::vector<double>& x,
                     const std::vector<int>& x_int, std::ostream* msgs)
      : f_(f),
        y0_(y0),
        y0_dbl_(value_of(y0)),
        theta_(theta),
        theta_dbl_(value_of(theta)),
        x_(x),
        x_int_(x_int),
        N_(y0.size()),
        M_(theta.size()),
        size_(N_ + N_ * (N_ + M_)),
        msgs_(msgs) {}

  /**
   * Populates the derivative vector with derivatives of the
   * coupled ODE system state with respect to time evaluated at the
   * specified state and specified time.
   *
   * @param[in]  z the current state of the coupled, shifted ode system,
   * of size <code>size()</code>.
   * @param[in, out] dz_dt populate with the derivatives of the
   * coupled system evaluated at the specified state and time.
   * @param[in] t time.
   * @throw exception if the base system does not return a
   * derivative vector of the same size as the state vector.
   *
   * y is the base ODE system state
   *
   */
  void operator()(const std::vector<double>& z, std::vector<double>& dz_dt,
                  double t) const {
    using std::vector;

    try {
      start_nested();

      vector<var> y_vars(z.begin(), z.begin() + N_);

      vector<var> theta_vars(theta_dbl_.begin(), theta_dbl_.end());

      vector<var> dy_dt_vars = f_(t, y_vars, theta_vars, x_, x_int_, msgs_);

      check_size_match("coupled_ode_system", "dz_dt", dy_dt_vars.size(),
                       "states", N_);

      for (size_t i = 0; i < N_; i++) {
        dz_dt[i] = dy_dt_vars[i].val();
        dy_dt_vars[i].grad();

        for (size_t j = 0; j < N_; j++) {
          // orders derivatives by equation (i.e. if there are 2 eqns
          // (y1, y2) and 2 parameters (a, b), dy_dt will be ordered as:
          // dy1_dt, dy2_dt, dy1_da, dy2_da, dy1_db, dy2_db
          double temp_deriv = 0;
          const size_t offset = N_ + N_ * j;
          for (size_t k = 0; k < N_; k++)
            temp_deriv += z[offset + k] * y_vars[k].adj();

          dz_dt[offset + i] = temp_deriv;
        }

        for (size_t j = 0; j < M_; j++) {
          double temp_deriv = theta_vars[j].adj();
          const size_t offset = N_ + N_ * N_ + N_ * j;
          for (size_t k = 0; k < N_; k++)
            temp_deriv += z[offset + k] * y_vars[k].adj();

          dz_dt[offset + i] = temp_deriv;
        }

        set_zero_all_adjoints_nested();
      }
    } catch (const std::exception& e) {
      recover_memory_nested();
      throw;
    }
    recover_memory_nested();
  }

  /**
   * Returns the size of the coupled system.
   *
   * @return size of the coupled system.
   */
  size_t size() const { return size_; }

  /**
   * Returns the initial state of the coupled system.
   *
   * Because the initial state is unknown, the coupled system
   * incorporates the initial condition offset from zero as
   * a parameter, and hence the return of this function is a
   * vector of zeros.
   *
   * @return the initial condition of the coupled system.  This is
   * a vector of length size() where all elements are 0.
   */
  std::vector<double> initial_state() const {
    std::vector<double> initial(size_, 0.0);
    for (size_t i = 0; i < N_; i++)
      initial[i] = y0_dbl_[i];
    for (size_t i = 0; i < N_; i++)
      initial[N_ + i * N_ + i] = 1.0;
    return initial;
  }

  /**
   * Return the basic ODE solutions given the specified coupled
   * system solutions, including the partials versus the
   * parameters encoded in the autodiff results.
   *
   * @param y the vector of the coupled states after solving the ode
   */
  std::vector<std::vector<var> > decouple_states(
      const std::vector<std::vector<double> >& y) const {
    using std::vector;

    vector<var> vars = y0_;
    vars.insert(vars.end(), theta_.begin(), theta_.end());

    vector<var> temp_vars(N_);
    vector<double> temp_gradients(N_ + M_);
    vector<vector<var> > y_return(y.size());

    for (size_t i = 0; i < y.size(); i++) {
      // iterate over number of equations
      for (size_t j = 0; j < N_; j++) {
        // iterate over parameters for each equation
        for (size_t k = 0; k < N_ + M_; k++)
          temp_gradients[k] = y[i][N_ + N_ * k + j];

        temp_vars[j] = precomputed_gradients(y[i][j], vars, temp_gradients);
      }
      y_return[i] = temp_vars;
    }

    return y_return;
  }
};

}  // namespace math
}  // namespace stan
#endif
