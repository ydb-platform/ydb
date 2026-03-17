#ifndef STAN_LANG_AST_ROW_VECTOR_LOCAL_TYPE_HPP
#define STAN_LANG_AST_ROW_VECTOR_LOCAL_TYPE_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
namespace lang {

/**
 * Row vector local var type.
 */
struct row_vector_local_type {
  /**
   * Row vector length
   */
  expression N_;

  /**
   * Construct a local var type with default values.
   */
  row_vector_local_type();

  /**
   * Construct a local var type with specified values.
   * Arg `N` should be int expression - constructor doesn't check.
   *
   * @param N num columns for row vector
   */
  explicit row_vector_local_type(const expression& N);

  /**
   * Get N (num cols).
   */
  expression N() const;
};
}  // namespace lang
}  // namespace stan
#endif
