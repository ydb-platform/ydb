#ifndef STAN_LANG_AST_NODE_VARIABLE_DEF_HPP
#define STAN_LANG_AST_NODE_VARIABLE_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

    variable::variable() { }

    variable::variable(const std::string& name) : name_(name) { }

    void variable::set_type(const bare_expr_type& bare_type) {
      type_ = bare_type;
    }

  }
}
#endif
