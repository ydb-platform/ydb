#ifndef STAN_LANG_AST_SIMPLEX_BLOCK_TYPE_HPP
#define STAN_LANG_AST_SIMPLEX_BLOCK_TYPE_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
namespace lang {

/**
 * Simplex block var type.
 */
struct simplex_block_type {
  /**
   * Simplex vector
   */
  expression K_;

  /**
   * Construct a block var type with default values.
   */
  simplex_block_type();

  /**
   * Construct a block var type with specified values.
   *
   * @param K num columns for simplex
   */
  explicit simplex_block_type(const expression& K);

  /**
   * Get K (num columns)
   */
  expression K() const;
};

}  // namespace lang
}  // namespace stan
#endif
