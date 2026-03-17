#ifndef STAN_LANG_AST_NODE_ALGEBRA_SOLVER_CONTROL_HPP
#define STAN_LANG_AST_NODE_ALGEBRA_SOLVER_CONTROL_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <string>

namespace stan {
  namespace lang {

    struct expression;

    /**
     * Structure for an algebra solver statement with control
     * parameters for the solver.
     */
    struct algebra_solver_control {
      /**
       * Name of the algebra system.
       */
      std::string system_function_name_;

      /**
       * Initial guess (vector of real).
       */
      expression y_;

      /**
       * Parameters (vector of real).
       */
      expression theta_;

      /**
       * Real-valued data (array of real).
       */
      expression x_r_;

      /**
       * Integer-valued data (array of int).
       */
      expression x_i_;

      /**
       * Relative tolerance (real).
       */
      expression rel_tol_;

      /**
       * Function tolerance (real).
       */
      expression fun_tol_;

      /**
       * Maximum number of steps (integer).
       */
      expression max_num_steps_;

      /**
       * Construct a default algebra solver object with control.
       */
      algebra_solver_control();

      /**
       * Construt an algebraic solver with control parameters with
       * the specified values.
       *
       * @param system_function_name name of algebraic solver
       * @param y initial guess for solution
       * @param theta parameters
       * @param x_r real-valued data
       * @param x_i integer-valued data
       * @param rel_tol relative tolerance of integrator
       * @param fun_tol function tolerance of integrator
       * @param max_num_steps max steps in integrator
       */
       algebra_solver_control(const std::string& system_function_name,
                              const expression& y,
                              const expression& theta,
                              const expression& x_r,
                              const expression& x_i,
                              const expression& rel_tol,
                              const expression& fun_tol,
                              const expression& max_num_steps);
    };

  }
}
#endif
