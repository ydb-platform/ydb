#ifndef STAN_LANG_AST_NODE_REJECT_STATEMENT_DEF_HPP
#define STAN_LANG_AST_NODE_REJECT_STATEMENT_DEF_HPP

#include <stan/lang/ast.hpp>
#include <vector>

namespace stan {
  namespace lang {

    reject_statement::reject_statement() { }

    reject_statement::reject_statement(const std::vector<printable>& printables)
      : printables_(printables) { }

  }
}
#endif
