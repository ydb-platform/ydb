#ifndef STAN_LANG_AST_NODE_INTEGRATE_ODE_CONTROL_DEF_HPP
#define STAN_LANG_AST_NODE_INTEGRATE_ODE_CONTROL_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

    integrate_ode_control::integrate_ode_control() { }

    integrate_ode_control::integrate_ode_control(
                           const std::string& integration_function_name,
                           const std::string& system_function_name,
                           const expression& y0,  const expression& t0,
                           const expression& ts, const expression& theta,
                           const expression& x, const expression& x_int,
                           const expression& rel_tol, const expression& abs_tol,
                           const expression& max_num_steps)
      : integration_function_name_(integration_function_name),
        system_function_name_(system_function_name),
        y0_(y0), t0_(t0), ts_(ts), theta_(theta), x_(x), x_int_(x_int),
        rel_tol_(rel_tol), abs_tol_(abs_tol), max_num_steps_(max_num_steps) {
    }

  }
}
#endif
