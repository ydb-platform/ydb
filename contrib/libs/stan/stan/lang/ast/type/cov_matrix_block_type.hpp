#ifndef STAN_LANG_AST_COV_MATRIX_BLOCK_TYPE_HPP
#define STAN_LANG_AST_COV_MATRIX_BLOCK_TYPE_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
namespace lang {

/**
 * Covariance matrix block var type.
 */
struct cov_matrix_block_type {
  /**
   * Number of rows and columns
   */
  expression K_;

  /**
   * Construct a block var type with default values.
   */
  cov_matrix_block_type();

  /**
   * Construct a block var type with specified values.
   * Size should be int expression - constructor doesn't check.
   *
   * @param K cov matrix size
   */
  explicit cov_matrix_block_type(const expression& K);

  /**
   * Get K (cov matrix num rows, columns)
   */
  expression K() const;
};

}  // namespace lang
}  // namespace stan
#endif
