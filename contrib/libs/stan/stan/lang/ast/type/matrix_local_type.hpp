#ifndef STAN_LANG_AST_MATRIX_LOCAL_TYPE_HPP
#define STAN_LANG_AST_MATRIX_LOCAL_TYPE_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
  namespace lang {

    /**
     * Matrix local var type.
     */
    struct matrix_local_type {
      /**
       * Number of rows
       */
      expression M_;

      /**
       * Number of columns
       */
      expression N_;

      /**
       * Construct a local var type with default values.
       */
      matrix_local_type();

      /**
       * Construct a local var type with specified values.
       * Sizes should be int expressions - constructor doesn't check.
       *
       * @param M num rows
       * @param N num columns
       */
      matrix_local_type(const expression& M,
                        const expression& N);

      /**
       * Get M (num rows).
       */
      expression M() const;

      /**
       * Get N (num cols).
       */
      expression N() const;
    };

  }
}
#endif
