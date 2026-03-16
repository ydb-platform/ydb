#ifndef STAN_LANG_AST_NODE_MATRIX_EXPR_DEF_HPP
#define STAN_LANG_AST_NODE_MATRIX_EXPR_DEF_HPP

#include <stan/lang/ast.hpp>
#include <vector>

namespace stan {
  namespace lang {

    matrix_expr::matrix_expr()
      : args_(), has_var_(false), matrix_expr_scope_() { }

    matrix_expr::matrix_expr(const std::vector<expression>& args)
      : args_(args), has_var_(false), matrix_expr_scope_() { }

  }
}
#endif
