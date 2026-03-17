#ifndef STAN_LANG_AST_NODE_WHILE_STATEMENT_DEF_HPP
#define STAN_LANG_AST_NODE_WHILE_STATEMENT_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    while_statement::while_statement() { }

    while_statement::while_statement(const expression& condition,
                                     const statement& body)
      : condition_(condition), body_(body) { }

  }
}
#endif
