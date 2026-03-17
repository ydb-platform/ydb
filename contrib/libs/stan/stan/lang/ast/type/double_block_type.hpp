#ifndef STAN_LANG_AST_DOUBLE_BLOCK_TYPE_HPP
#define STAN_LANG_AST_DOUBLE_BLOCK_TYPE_HPP

#include <stan/lang/ast/node/offset_multiplier.hpp>
#include <stan/lang/ast/node/range.hpp>

namespace stan {
namespace lang {

/**
 * Double block var type.
 */
struct double_block_type {
  /**
   * Bounds constraints
   */
  range bounds_;

  /**
   * Offset and multiplier
   */
  offset_multiplier ls_;

  /**
   * Construct a block var type with default values.
   */
  double_block_type();

  /**
   * Construct a block var type with specified values.
   *
   * @param bounds variable upper and/or lower bounds
   * @param ls variable offset and multiplier
   */
  explicit double_block_type(const range &bounds, const offset_multiplier &ls);

  /**
   * Construct a block var type with specified values.
   *
   * @param bounds variable upper and/or lower bounds
   */
  explicit double_block_type(const range &bounds);

  /**
   * Construct a block var type with specified values.
   *
   * @param ls variable offset and multiplier
   */
  explicit double_block_type(const offset_multiplier &ls);

  /**
   * Get bounds constraints.
   */
  range bounds() const;

  /**
   * Get offset and multiplier.
   */
  offset_multiplier ls() const;
};

}  // namespace lang
}  // namespace stan
#endif
