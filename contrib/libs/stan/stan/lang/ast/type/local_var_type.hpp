#ifndef STAN_LANG_AST_LOCAL_VAR_TYPE_HPP
#define STAN_LANG_AST_LOCAL_VAR_TYPE_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <string>
#include <vector>
#include <cstddef>

namespace stan {
namespace lang {

/**
 * Local variable types have sized container types.
 */
struct local_array_type;
struct double_type;
struct ill_formed_type;
struct int_type;
struct matrix_local_type;
struct row_vector_local_type;
struct vector_local_type;

struct local_var_type {
  /**
   * Recursive wrapper for local variable types.
   */
  typedef boost::variant<boost::recursive_wrapper<ill_formed_type>,
                         boost::recursive_wrapper<double_type>,
                         boost::recursive_wrapper<int_type>,
                         boost::recursive_wrapper<matrix_local_type>,
                         boost::recursive_wrapper<row_vector_local_type>,
                         boost::recursive_wrapper<vector_local_type>,
                         boost::recursive_wrapper<local_array_type> >
      local_t;

  /**
   * The local variable type held by this wrapper.
   */
  local_t var_type_;

  /**
   * Construct a bare var type with default values.
   */
  local_var_type();

  /**
   * Construct a local var type
   *
   * @param x local variable type raw variant type.
   */
  local_var_type(const local_var_type& x);  // NOLINT(runtime/explicit)

  /**
   * Construct a local var type with the specified type.
   *
   * @param x local variable type
   */
  local_var_type(const ill_formed_type& x);  // NOLINT(runtime/explicit)

  /**
   * Construct a local var type with the specified type.
   *
   * @param x local variable type
   */
  local_var_type(const double_type& x);  // NOLINT(runtime/explicit)

  /**
   * Construct a local var type with the specified type.
   *
   * @param x local variable type
   */

  local_var_type(const int_type& x);  // NOLINT(runtime/explicit)

  /**
   * Construct a local var type with the specified type.
   *
   * @param x local variable type
   */
  local_var_type(const matrix_local_type& x);  // NOLINT(runtime/explicit)

  /**
   * Construct a local var type with the specified type.
   *
   * @param x local variable type
   */
  local_var_type(
      const row_vector_local_type& x);  // NOLINT(runtime/explicit)

  /**
   * Construct a local var type with the specified type.
   *
   * @param x local variable type
   */
  local_var_type(const vector_local_type& x);  // NOLINT(runtime/explicit)

  /**
   * Construct a local var type with the specified type.
   *
   * @param x local variable type
   */
  local_var_type(const local_array_type& x);  // NOLINT(runtime/explicit)

  /**
   * Construct a local var type with the specified type.
   *
   * @param x local variable type
   */
  local_var_type(const local_t& x);  // NOLINT(runtime/explicit)

  /**
   * Returns expression for length of vector types or
   * number of rows for matrix type, nil otherwise.
   */
  expression arg1() const;

  /**
   * Returns expression for number of columns for matrix types,
   * nil otherwise.
   */
  expression arg2() const;

  /**
   * If `var_type` is `local_array_type`, returns the innermost type
   * contained in the array, otherwise will return `ill_formed_type`.
   */
  local_var_type array_contains() const;

  /**
   * Returns number of array dimensions for this type.
   * Returns 0 for non-array types.
   */
  int array_dims() const;

  /**
   * Returns array element type if `var_type_` is `local_array_type`,
   * ill_formed_type otherwise.  (Call `is_array_type()` first.)
   */
  local_var_type array_element_type() const;

  /**
   * Returns array length for local_array_type, nil otherwise.
   */
  expression array_len() const;

  /**
   * Returns vector of array lengths for local_array_type,
   * empty vector otherwise.
   */
  std::vector<expression> array_lens() const;

  /**
   * Returns equivalent bare_expr_type (unsized) for this local type.
   */
  bare_expr_type bare_type() const;

  /**
   * If array type, returns innermost type,
   * otherwise returns this type.
   */
  local_var_type innermost_type() const;

  /**
   * Returns true if `var_type_` is `local_array_type`, false otherwise.
   */
  bool is_array_type() const;

  /**
   * Returns Stan language type name.
   */
  std::string name() const;

  /**
   * Returns total number of dimensions for container type.
   * Returns 0 for scalar types.
   */
  int num_dims() const;
};

/**
 * Stream a user-readable version of the local_var_type to the
 * specified output stream, returning the specified argument
 * output stream to allow chaining.
 *
 * @param o output stream
 * @param x expression type
 * @return argument output stream
 */
std::ostream& operator<<(std::ostream& o, const local_var_type& x);

}  // namespace lang
}  // namespace stan
#endif
