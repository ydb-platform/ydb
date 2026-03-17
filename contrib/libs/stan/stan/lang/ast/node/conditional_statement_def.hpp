#ifndef STAN_LANG_AST_NODE_CONDITIONAL_STATEMENT_DEF_HPP
#define STAN_LANG_AST_NODE_CONDITIONAL_STATEMENT_DEF_HPP

#include <stan/lang/ast.hpp>
#include <vector>

namespace stan {
  namespace lang {

    conditional_statement::conditional_statement() {  }

    conditional_statement
    ::conditional_statement(const std::vector<expression>& conditions,
                            const std::vector<statement>& bodies)
      : conditions_(conditions), bodies_(bodies) { }



  }
}
#endif
