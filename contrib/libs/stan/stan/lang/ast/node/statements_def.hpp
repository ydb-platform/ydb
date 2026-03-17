#ifndef STAN_LANG_AST_NODE_STATEMENTS_DEF_HPP
#define STAN_LANG_AST_NODE_STATEMENTS_DEF_HPP

#include <stan/lang/ast.hpp>
#include <vector>

namespace stan {
  namespace lang {

    statements::statements() {  }

    statements::statements(const std::vector<local_var_decl>& local_decl,
                           const std::vector<statement>& stmts)
      : local_decl_(local_decl), statements_(stmts) { }

  }
}
#endif
