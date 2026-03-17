#ifndef STAN_LANG_AST_NODE_INTEGRATE_ODE_CONTROL_HPP
#define STAN_LANG_AST_NODE_INTEGRATE_ODE_CONTROL_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <string>

namespace stan {
  namespace lang {

    struct expression;

    /**
     * Structure for a diff eq integration statement with control
     * parameters for the integrator.
     */
    struct integrate_ode_control {
      /**
       * The name of the integrator.
       */
      std::string integration_function_name_;

      /**
       * Name of the ODE system.
       */
      std::string system_function_name_;

      /**
       * Initial state (array of real).
       */
      expression y0_;

      /**
       * Initial time (real).
       */
      expression t0_;

      /**
       * Solution times (array of real).
       */
      expression ts_;

      /**
       * Parameters (array of real).
       */
      expression theta_;

      /**
       * Real-valued data (array of real).
       */
      expression x_;

      /**
       * Integer-valued data (array of int).
       */
      expression x_int_;  // integer data

      /**
       * Relative tolerance (real).
       */
      expression rel_tol_;

      /**
       * Absolute tolerance (real).
       */
      expression abs_tol_;

      /**
       * Maximum number of steps (integer).
       */
      expression max_num_steps_;

      /**
       * Construct a default ODE integrator object with control.
       */
      integrate_ode_control();

      /**
       * Construt an ODE integrator with control parameter with the
       * specified values.
       *
       * @param integration_function_name name of integrator
       * @param system_function_name name of ODE system
       * @param y0 initial value
       * @param t0 initial time
       * @param ts solution times
       * @param theta parameters
       * @param x real-valued data
       * @param x_int integer-valued data
       * @param rel_tol relative tolerance of integrator
       * @param abs_tol absolute tolerance of integrator
       * @param max_steps max steps in integrator
       */
      integrate_ode_control(const std::string& integration_function_name,
                            const std::string& system_function_name,
                            const expression& y0,
                            const expression& t0,
                            const expression& ts,
                            const expression& theta,
                            const expression& x,
                            const expression& x_int,
                            const expression& rel_tol,
                            const expression& abs_tol,
                            const expression& max_steps);
    };

  }
}
#endif
