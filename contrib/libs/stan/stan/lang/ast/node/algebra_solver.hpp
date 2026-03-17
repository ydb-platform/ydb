#ifndef STAN_LANG_AST_NODE_ALGEBRA_SOLVER_HPP
#define STAN_LANG_AST_NODE_ALGEBRA_SOLVER_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <string>

namespace stan {
  namespace lang {

    struct expression;

    /**
     * Structure for algebraic solver statement.
     */
    struct algebra_solver {
      /**
       * Name of the algebra system.
       */
      std::string system_function_name_;

      /**
       * Initial guess for solution.
       */
      expression y_;

      /**
       * Parameters.
       */
      expression theta_;

      /**
       * Real-valued data.
       */
      expression x_r_;

      /**
       * Integer-valued data.
       */
      expression x_i_;

      /**
       * Construct a default algebra solver node.
       */
      algebra_solver();

      /**
       * Construct an algebraic solver.
       *
       * @param system_function_name name of ODE system
       * @param y initial guess for solution
       * @param theta parameters
       * @param x_r real-valued data
       * @param x_i integer-valued data
       */
      algebra_solver(const std::string& system_function_name,
                     const expression& y,
                     const expression& theta,
                     const expression& x_r,
                     const expression& x_i);
    };

  }
}
#endif
