#ifndef STAN_LANG_AST_NODE_PRINT_STATEMENT_DEF_HPP
#define STAN_LANG_AST_NODE_PRINT_STATEMENT_DEF_HPP

#include <stan/lang/ast.hpp>
#include <vector>

namespace stan {
  namespace lang {

    print_statement::print_statement() { }

    print_statement::print_statement(const std::vector<printable>& printables)
      : printables_(printables) { }

  }
}
#endif
