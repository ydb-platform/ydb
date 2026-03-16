#ifndef STAN_LANG_AST_NODE_ARRAY_EXPR_DEF_HPP
#define STAN_LANG_AST_NODE_ARRAY_EXPR_DEF_HPP

#include <stan/lang/ast.hpp>
#include <vector>

namespace stan {
namespace lang {

array_expr::array_expr()
    : args_(), type_(), has_var_(false), array_expr_scope_() {}

}  // namespace lang
}  // namespace stan
#endif
