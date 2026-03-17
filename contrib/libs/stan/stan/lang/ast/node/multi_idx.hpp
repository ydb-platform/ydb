#ifndef STAN_LANG_AST_NODE_MULTI_IDX_HPP
#define STAN_LANG_AST_NODE_MULTI_IDX_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
  namespace lang {

    struct multi_idx {
      /**
       * Multiple indexes (array of integers).
       */
      expression idxs_;

      /**
       * Construct a default (nil) multi-index.
       */
      multi_idx();

      /**
       * Construct a multiple index from the specified indexes.
       *
       * @param idxs indexes expression (array of integers)
       */
      multi_idx(const expression& idxs);  // NOLINT(runtime/explicit)
    };

  }
}
#endif
