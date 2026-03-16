#ifndef STAN_LANG_AST_CHOLESKY_FACTOR_COV_BLOCK_TYPE_HPP
#define STAN_LANG_AST_CHOLESKY_FACTOR_COV_BLOCK_TYPE_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
  namespace lang {

    /**
     * Cholesky factor for covariance matrix block var type.
     *
     * Note:  no 1-arg constructor for square matrix;
     * both row and column dimensions always required.
     */
    struct cholesky_factor_cov_block_type {
      /**
       * Number of rows.
       */
      expression M_;

      /**
       * Number of columns.
       */
      expression N_;

      /**
       * Construct a block var type with default values.
       */
      cholesky_factor_cov_block_type();

      /**
       * Construct a block var type with specified values.
       *
       * @param M num rows
       * @param N num columns
       */
      cholesky_factor_cov_block_type(const expression& M,
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
