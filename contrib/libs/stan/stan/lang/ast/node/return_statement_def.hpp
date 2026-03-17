#ifndef STAN_LANG_AST_NODE_RETURN_STATEMENT_DEF_HPP
#define STAN_LANG_AST_NODE_RETURN_STATEMENT_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    return_statement::return_statement() { }

    return_statement::return_statement(const expression& expr)
      : return_value_(expr) { }

  }
}
#endif
