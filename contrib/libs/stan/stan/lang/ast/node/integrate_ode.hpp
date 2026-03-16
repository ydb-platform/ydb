#ifndef STAN_LANG_AST_NODE_INTEGRATE_ODE_HPP
#define STAN_LANG_AST_NODE_INTEGRATE_ODE_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Structure for integrate diff eq statement.
     */
    struct integrate_ode {
      /**
       * The name of the integrator.
       */
      std::string integration_function_name_;

      /**
       * Name of the ODE system.
       */
      std::string system_function_name_;

      /**
       * Initial state.
       */
      expression y0_;

      /**
       * Initial time.
       */
      expression t0_;

      /**
       * Solution times.
       */
      expression ts_;

      /**
       * Parameters.
       */
      expression theta_;  // params

      /**
       * Real-valued data.
       */
      expression x_;

      /**
       * Integer-valued data.
       */
      expression x_int_;

      /**
       * Construct a default integrate ODE node.
       */
      integrate_ode();

      /**
       * Construct an integrate ODE node with the specified
       * components.
       *
       * @param integration_function_name name of integrator
       * @param system_function_name name of ODE system
       * @param y0 initial value
       * @param t0 initial time
       * @param ts solution times
       * @param theta parameters
       * @param x real-valued data
       * @param x_int integer-valued data
       */
      integrate_ode(const std::string& integration_function_name,
                    const std::string& system_function_name,
                    const expression& y0,
                    const expression& t0,
                    const expression& ts,
                    const expression& theta,
                    const expression& x,
                    const expression& x_int);
    };

  }
}
#endif
