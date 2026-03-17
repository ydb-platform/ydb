#ifndef STAN_MATH_PRIM_ARR_FUNCTOR_COUPLED_ODE_SYSTEM_HPP
#define STAN_MATH_PRIM_ARR_FUNCTOR_COUPLED_ODE_SYSTEM_HPP

#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <ostream>
#include <vector>

namespace stan {
namespace math {

/**
 * Base template class for a coupled ordinary differential equation
 * system, which adds sensitivities to the base system.
 *
 * This template class declaration should not be instantiated
 * directly --- it is just here to serve as a base for its
 * specializations, some of which are defined in namespace
 * <code>stan::math</code>.
 *
 * @tparam F the functor for the base ode system
 * @tparam T1 type of the initial state
 * @tparam T2 type of the parameters
 */
template <typename F, typename T1, typename T2>
struct coupled_ode_system {};

/**
 * The coupled ode system for known initial values and known
 * parameters.
 *
 * <p>This coupled system does not add anything to the base
 * system used to construct it, but is here for generality of the
 * integration implementation.
 *
 * @tparam F type of system function for the base ODE system.
 */
template <typename F>
class coupled_ode_system<F, double, double> {
 public:
  const F& f_;
  const std::vector<double>& y0_dbl_;
  const std::vector<double>& theta_dbl_;
  const std::vector<double>& x_;
  const std::vector<int>& x_int_;
  const size_t N_;
  const size_t M_;
  const size_t size_;
  std::ostream* msgs_;

  /**
   * Construct the coupled ODE system from the base system
   * function, initial state, parameters, data and a stream for
   * messages.
   *
   * @param[in] f base ode system functor.
   * @param[in] y0 initial state of the base ode.
   * @param[in] theta parameters of the base ode.
   * @param[in] x  real data.
   * @param[in] x_int integer data.
   * @param[in, out] msgs print stream.
   */
  coupled_ode_system(const F& f, const std::vector<double>& y0,
                     const std::vector<double>& theta,
                     const std::vector<double>& x,
                     const std::vector<int>& x_int, std::ostream* msgs)
      : f_(f),
        y0_dbl_(y0),
        theta_dbl_(theta),
        x_(x),
        x_int_(x_int),
        N_(y0.size()),
        M_(theta.size()),
        size_(N_),
        msgs_(msgs) {}

  /**
   * Calculates the derivative of the coupled ode system with
   * respect to the specified state at the specified time using
   * the system state function.
   *
   * The derivative vector created is the same length as the
   * length as the state vector.
   *
   * @param[in] y current state of the coupled ode.
   * @param[out] dy_dt populated with derivatives of the coupled
   * system evaluated at specified state and time.
   * @param[in] t time.
   * @throw exception if the system function does not return
   * a derivative vector of the same size as the state vector.
   */
  void operator()(const std::vector<double>& y, std::vector<double>& dy_dt,
                  double t) const {
    dy_dt = f_(t, y, theta_dbl_, x_, x_int_, msgs_);
    check_size_match("coupled_ode_system", "y", y.size(), "dy_dt",
                     dy_dt.size());
  }

  /**
   * Returns the size of the coupled system.
   *
   * @return size of the coupled system.
   */
  int size() const { return size_; }

  /**
   * Returns the initial state of the coupled system, which is
   * identical to the base ODE original state in this
   * implementation because the initial state is known.
   *
   * The return value is a vector of length size() where the first
   * N (base ode system size) parameters are the initial
   * conditions of the base ode system and the rest of the initial
   * conditions is 0.
   *
   * @return initial state of the coupled system
   */
  std::vector<double> initial_state() const {
    std::vector<double> state(size_, 0.0);
    for (size_t n = 0; n < N_; n++)
      state[n] = y0_dbl_[n];
    return state;
  }

  /**
   * Returns the base portion of the coupled state.
   *
   * <p>In this class's implementation, the coupled system is
   * equivalent to the base system, so this function just returns
   * its input.
   *
   * @param y the vector of the coupled states after solving the ode
   * @return the decoupled states
   */
  std::vector<std::vector<double> > decouple_states(
      const std::vector<std::vector<double> >& y) const {
    return y;
  }
};

}  // namespace math
}  // namespace stan
#endif
