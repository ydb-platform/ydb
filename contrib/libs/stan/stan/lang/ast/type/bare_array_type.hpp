#ifndef STAN_LANG_AST_BARE_ARRAY_TYPE_HPP
#define STAN_LANG_AST_BARE_ARRAY_TYPE_HPP

#include <stan/lang/ast/type/bare_expr_type.hpp>
#include <string>

namespace stan {
namespace lang {

struct bare_expr_type;

/**
 * Bare array type for Stan variables and expressions (recursive).
 */
struct bare_array_type {
  /**
   * The array element type.
   */
  bare_expr_type element_type_;

  /**
   * True if variable type declared with "data" qualifier.
   */
  bool is_data_;

  /**
   * Construct a bare array type with default values.
   */
  bare_array_type();

  /**
   * Construct a bare array type with the specified element type.
   *
   * @param el_type element type
   */
  explicit bare_array_type(const bare_expr_type& el_type);

  /**
   * Construct a bare array type with the specified element type
   * and the specified number of dimensions.
   *
   * @param el_type element type
   * @param num_dims
   */
  bare_array_type(const bare_expr_type& el_type, size_t num_dims);

  /**
   * Returns type of elements stored in innermost array.
   */
  bare_expr_type contains() const;

  /**
   * Returns number of array dimensions for well-formed types.
   * Returns 0 if element type is ill-formed.
   */
  int dims() const;

  /**
   * Returns identity string for this type.
   */
  std::string oid() const;
};
}  // namespace lang
}  // namespace stan
#endif
