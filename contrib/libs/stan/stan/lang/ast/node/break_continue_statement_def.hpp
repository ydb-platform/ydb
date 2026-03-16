#ifndef STAN_LANG_AST_NODE_BREAK_CONTINUE_STATEMENT_DEF_HPP
#define STAN_LANG_AST_NODE_BREAK_CONTINUE_STATEMENT_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

    break_continue_statement::break_continue_statement() { }

    break_continue_statement::break_continue_statement(const std::string& s)
      : generate_(s) { }

  }
}
#endif
