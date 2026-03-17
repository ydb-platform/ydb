#ifndef STAN_LANG_AST_NODE_DOUBLE_LITERAL_HPP
#define STAN_LANG_AST_NODE_DOUBLE_LITERAL_HPP

#include <stan/lang/ast/type/bare_expr_type.hpp>
#include <string>
#include <cstddef>

namespace stan {
namespace lang {

/**
 * Node for holding a double literal.
 */
struct double_literal {
  /**
   * Value of literal.
   */
  double val_;

  /**
   * String representation.
   */
  std::string string_;

  /**
   * Expression type.
   */
  bare_expr_type type_;

  /**
   * Default constructor for double literal.
   */
  double_literal();

  /**
   * Construct a double literal with the specified value.
   *
   * @param val value of literal
   */
  explicit double_literal(double val);
};

}  // namespace lang
}  // namespace stan
#endif
