#ifndef STAN_LANG_AST_CHOLESKY_FACTOR_CORR_BLOCK_TYPE_HPP
#define STAN_LANG_AST_CHOLESKY_FACTOR_CORR_BLOCK_TYPE_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
namespace lang {

/**
 * Cholesky factor for a correlation matrix block var type.
 */
struct cholesky_factor_corr_block_type {
  /**
   * Number of rows and columns.
   */
  expression K_;

  /**
   * Construct a block var type with default values.
   */
  cholesky_factor_corr_block_type();

  /**
   * Construct a block var type with specified values.
   *
   * @param K corr matrix num rows, columns
   */
  explicit cholesky_factor_corr_block_type(
      const expression& K);

  /**
   * Get K (corr matrix num rows, columns)
   */
  expression K() const;
};

}  // namespace lang
}  // namespace stan
#endif
