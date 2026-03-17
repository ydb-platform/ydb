#ifndef STAN_LANG_AST_NODE_INCREMENT_LOG_PROB_STATEMENT_DEF_HPP
#define STAN_LANG_AST_NODE_INCREMENT_LOG_PROB_STATEMENT_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    increment_log_prob_statement::increment_log_prob_statement() { }

    increment_log_prob_statement::increment_log_prob_statement(
                                               const expression& log_prob)
      : log_prob_(log_prob) {  }

  }
}
#endif
