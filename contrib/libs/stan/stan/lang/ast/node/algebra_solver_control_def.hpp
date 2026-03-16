#ifndef STAN_LANG_AST_NODE_ALGEBRA_SOLVER_CONTROL_DEF_HPP
#define STAN_LANG_AST_NODE_ALGEBRA_SOLVER_CONTROL_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

    algebra_solver_control::algebra_solver_control() { }

    algebra_solver_control::algebra_solver_control(
                            const std::string& system_function_name,
                            const expression& y,
                            const expression& theta,
                            const expression& x_r,
                            const expression& x_i,
                            const expression& rel_tol,
                            const expression& fun_tol,
                            const expression& max_num_steps)
    : system_function_name_(system_function_name),
      y_(y), theta_(theta), x_r_(x_r), x_i_(x_i),
      rel_tol_(rel_tol), fun_tol_(fun_tol), max_num_steps_(max_num_steps) {
    }

  }
}
#endif
