#ifndef STAN_LANG_AST_CORR_MATRIX_BLOCK_TYPE_HPP
#define STAN_LANG_AST_CORR_MATRIX_BLOCK_TYPE_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
namespace lang {

/**
 * Correlation matrix block var type.
 */
struct corr_matrix_block_type {
  /**
   * Number of rows and columns
   */
  expression K_;

  /**
   * Construct a block var type with default values.
   */
  corr_matrix_block_type();

  /**
   * Construct a block var type with specified values.
   * Size should be int expression - constructor doesn't check.
   *
   * @param K corr matrix size
   */
  explicit corr_matrix_block_type(const expression& K);

  /**
   * Get K (corr matrix num rows, columns)
   */
  expression K() const;
};

}  // namespace lang
}  // namespace stan
#endif
