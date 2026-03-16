#ifndef STAN_LANG_AST_NODE_ROW_VECTOR_EXPR_DEF_HPP
#define STAN_LANG_AST_NODE_ROW_VECTOR_EXPR_DEF_HPP

#include <stan/lang/ast.hpp>
#include <vector>

namespace stan {
  namespace lang {

    row_vector_expr::row_vector_expr()
      : args_(), has_var_(false), row_vector_expr_scope_() { }

    row_vector_expr::row_vector_expr(const std::vector<expression>& args)
      : args_(args), has_var_(false), row_vector_expr_scope_() { }

  }
}
#endif
