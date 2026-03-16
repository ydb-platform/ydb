#ifndef STAN_LANG_AST_NODE_INTEGRATE_1D_HPP
#define STAN_LANG_AST_NODE_INTEGRATE_1D_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <string>

namespace stan {
namespace lang {

struct integrate_1d {
  /**
   * Name of the function being integrated.
   */
  std::string function_name_;

  /**
   * Lower integration boundary.
   */
  expression lb_;

  /**
   * Upper integration boundary.
   */
  expression ub_;

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
   * Relative tolerance of solution.
   */
  expression rel_tol_;


  /**
   * Construct a 1D integrator AST node.
   */
  integrate_1d();

  /**
   * Construct a 1D integrator AST node with the specified function
   * name, lower and upper integration bounds, parameters, and real
   * and integer data.
   *
   * @param function_name name of function to integrate
   * @param lb lower bound of integration
   * @param ub upper bound of integration
   * @param theta parameters
   * @param x_r real data
   * @param x_i integer data
   * @param rel_tol relative tolerance
   */
  integrate_1d(const std::string& function_name, const expression& lb,
               const expression& ub, const expression& theta,
               const expression& x_r, const expression& x_i,
               const expression& rel_tol);
};

}
}
#endif
