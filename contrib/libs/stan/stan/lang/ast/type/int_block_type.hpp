#ifndef STAN_LANG_AST_INT_BLOCK_TYPE_HPP
#define STAN_LANG_AST_INT_BLOCK_TYPE_HPP

#include <stan/lang/ast/node/range.hpp>

namespace stan {
namespace lang {

/**
 * Integer block var type.
 */
struct int_block_type {
  /**
   * Bounds constraints
   */
  range bounds_;

  /**
   * Construct a block var type with default values.
   */
  int_block_type();

  /**
   * Construct a block var type with specified values.
   *
   * @param bounds variable upper and/or lower bounds
   */
  explicit int_block_type(const range& bounds);

  /**
   * Get bounds constraints.
   */
  range bounds() const;
};

}  // namespace lang
}  // namespace stan
#endif
