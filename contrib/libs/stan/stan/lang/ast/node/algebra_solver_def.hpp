#ifndef STAN_LANG_AST_NODE_ALGEBRA_SOLVER_DEF_HPP
#define STAN_LANG_AST_NODE_ALGEBRA_SOLVER_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

    algebra_solver::algebra_solver() { }

    algebra_solver::algebra_solver(const std::string& system_function_name,
                                   const expression& y,
                                   const expression& theta,
                                   const expression& x_r,
                                   const expression& x_i)
      : system_function_name_(system_function_name),
        y_(y), theta_(theta), x_r_(x_r), x_i_(x_i) { }

    }
}

#endif
