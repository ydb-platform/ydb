#ifndef STAN_LANG_AST_POSITIVE_ORDERED_BLOCK_TYPE_HPP
#define STAN_LANG_AST_POSITIVE_ORDERED_BLOCK_TYPE_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
namespace lang {

/**
 * Positive ordered block var type.
 */
struct positive_ordered_block_type {
  /**
   * Size of positive ordered vector
   */
  expression K_;

  /**
   * Construct a block var type with default values.
   */
  positive_ordered_block_type();

  /**
   * Construct a block var type with specified values.
   * Size should be int expression - constructor doesn't check.
   *
   * @param K size
   */
  explicit positive_ordered_block_type(const expression& K);

  /**
   * Get K (num cols).
   */
  expression K() const;
};

}  // namespace lang
}  // namespace stan
#endif
