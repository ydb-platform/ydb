#ifndef STAN_LANG_AST_NODE_INTEGRATE_ODE_DEF_HPP
#define STAN_LANG_AST_NODE_INTEGRATE_ODE_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

    integrate_ode::integrate_ode() { }

    integrate_ode::integrate_ode(const std::string& integration_function_name,
                                 const std::string& system_function_name,
                                 const expression& y0, const expression& t0,
                                 const expression& ts, const expression& theta,
                                 const expression& x, const expression& x_int)
      : integration_function_name_(integration_function_name),
        system_function_name_(system_function_name),
        y0_(y0), t0_(t0), ts_(ts), theta_(theta), x_(x), x_int_(x_int) {  }

  }
}
#endif
