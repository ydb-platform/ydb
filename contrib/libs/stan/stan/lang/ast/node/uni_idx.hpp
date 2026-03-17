#ifndef STAN_LANG_AST_NODE_UNI_IDX_HPP
#define STAN_LANG_AST_NODE_UNI_IDX_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
  namespace lang {

    /**
     * AST structure to hold a single array or matrix/vector index. 
     */
    struct uni_idx {
      /**
       * Index.
       */
      expression idx_;

      /**
       * Construct a default unary index.
       */
      uni_idx();

      /**
       * Construct a unary index with the specified value.
       *
       * @param idx index
       */
      uni_idx(const expression& idx);  // NOLINT(runtime/explicit)
    };

  }
}
#endif
