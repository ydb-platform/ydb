#ifndef STAN_LANG_AST_ORDERED_BLOCK_TYPE_HPP
#define STAN_LANG_AST_ORDERED_BLOCK_TYPE_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <stan/lang/ast/node/range.hpp>

namespace stan {
namespace lang {

/**
 * Ordered block var type.
 */
struct ordered_block_type {
  /**
   * Length of ordered vector
   */
  expression K_;

  /**
   * Construct a block var type with default values.
   */
  ordered_block_type();

  /**
   * Construct a block var type with specified values.
   * Size should be int expression - constructor doesn't check.
   *
   * @param K size
   */
  explicit ordered_block_type(const expression& K);

  /**
   * Get K (num cols).
   */
  expression K() const;
};

}  // namespace lang
}  // namespace stan
#endif
