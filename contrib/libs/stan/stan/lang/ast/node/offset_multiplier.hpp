#ifndef STAN_LANG_AST_NODE_OFFSET_MULTIPLIER_HPP
#define STAN_LANG_AST_NODE_OFFSET_MULTIPLIER_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
namespace lang {

/**
 * AST structure for a offset_multiplier object with a offset and multiplier
 * value.
 */
struct offset_multiplier {
  /**
   * Offset of offset-multiplier pair with <code>nil</code> value if only
   * multiplier.
   */
  expression offset_;

  /**
   * Multiplier of offset-multiplier pair with <code>nil</code> value if only
   * offset.
   */
  expression multiplier_;

  /**
   * Construct a default offset_multiplier object.
   */
  offset_multiplier();

  /**
   * Construct a offset_multiplier object with the specified offset and
   * multiplier.
   *
   * @param offset offset
   * @param multiplier multiplier
   */
  offset_multiplier(const expression &offset, const expression &multiplier);

  /**
   * Return true if the offset is non-nil.
   *
   * @return true if there is a offset
   */
  bool has_offset() const;

  /**
   * Return true if the multiplier is non-nil.
   *
   * @return true if there is a multiplier
   */
  bool has_multiplier() const;
};

}  // namespace lang
}  // namespace stan
#endif
