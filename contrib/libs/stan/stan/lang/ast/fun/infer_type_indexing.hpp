#ifndef STAN_LANG_AST_FUN_INFER_TYPE_INDEXING_HPP
#define STAN_LANG_AST_FUN_INFER_TYPE_INDEXING_HPP

#include <stan/lang/ast/type/bare_expr_type.hpp>

namespace stan {
  namespace lang {

    /**
     * Return the expression type resulting from indexing the
     * specified expression with the specified number of indexes. 
     *
     * @param e type of the variable being indexed
     * @param num_indexes number of indexes provided
     * @return bare_expr_type of indexed expression
     */
    bare_expr_type infer_type_indexing(const bare_expr_type& e,
                                       size_t num_indexes);
  }
}
#endif
