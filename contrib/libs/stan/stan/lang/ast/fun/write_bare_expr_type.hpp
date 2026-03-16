#ifndef STAN_LANG_AST_FUN_WRITE_BARE_EXPR_TYPE_HPP
#define STAN_LANG_AST_FUN_WRITE_BARE_EXPR_TYPE_HPP

#include <stan/lang/ast/type/bare_expr_type.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Write a user-readable version of the specified bare expression
     * type to the specified output stream.
     *
     * @param o output stream
     * @param type bare expression type
     */
    std::ostream& write_bare_expr_type(std::ostream& o, bare_expr_type type);
  }
}
#endif
