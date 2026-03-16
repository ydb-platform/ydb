#ifndef STAN_LANG_AST_BLOCK_VAR_TYPE_HPP
#define STAN_LANG_AST_BLOCK_VAR_TYPE_HPP

#include <boost/variant/recursive_variant.hpp>
#include <stan/lang/ast/node/expression.hpp>
#include <stan/lang/ast/node/offset_multiplier.hpp>
#include <stan/lang/ast/node/range.hpp>
#include <cstddef>
#include <string>
#include <vector>

namespace stan {
namespace lang {

/**
 * Block variable types
 */

struct block_array_type;
struct cholesky_factor_corr_block_type;
struct cholesky_factor_cov_block_type;
struct corr_matrix_block_type;
struct cov_matrix_block_type;
struct double_block_type;
struct ill_formed_type;
struct int_block_type;
struct matrix_block_type;
struct ordered_block_type;
struct positive_ordered_block_type;
struct row_vector_block_type;
struct simplex_block_type;
struct unit_vector_block_type;
struct vector_block_type;

struct block_var_type {
  /**
   * Recursive wrapper for block variable types.
   */
  typedef boost::variant<
      boost::recursive_wrapper<ill_formed_type>,
      boost::recursive_wrapper<cholesky_factor_corr_block_type>,
      boost::recursive_wrapper<cholesky_factor_cov_block_type>,
      boost::recursive_wrapper<corr_matrix_block_type>,
      boost::recursive_wrapper<cov_matrix_block_type>,
      boost::recursive_wrapper<double_block_type>,
      boost::recursive_wrapper<int_block_type>,
      boost::recursive_wrapper<matrix_block_type>,
      boost::recursive_wrapper<ordered_block_type>,
      boost::recursive_wrapper<positive_ordered_block_type>,
      boost::recursive_wrapper<row_vector_block_type>,
      boost::recursive_wrapper<simplex_block_type>,
      boost::recursive_wrapper<unit_vector_block_type>,
      boost::recursive_wrapper<vector_block_type>,
      boost::recursive_wrapper<block_array_type>>
      block_t;

  /**
   * The block variable type held by this wrapper.
   */
  block_t var_type_;

  /**
   * Construct a block var type with default values.
   */
  block_var_type();

  /**
   * Construct a block var type with the specified type.
   *
   * @param type block variable type
   */
  block_var_type(const block_var_type &type);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(const ill_formed_type &x);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(
      const cholesky_factor_corr_block_type &x);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(
      const cholesky_factor_cov_block_type &x);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(const corr_matrix_block_type &x);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(const cov_matrix_block_type &x);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(const double_block_type &x);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(const int_block_type &x);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(const matrix_block_type &x);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(const ordered_block_type &x);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(
      const positive_ordered_block_type &x);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(const row_vector_block_type &x);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(const simplex_block_type &x);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(const unit_vector_block_type &x);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(const vector_block_type &x);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(const block_array_type &x);  // NOLINT(runtime/explicit)

  /**
   * Construct a block var type with the specified type.
   *
   * @param x block variable type
   */
  block_var_type(const block_t &x);  // NOLINT(runtime/explicit)

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
   * If `var_type` is `block_array_type`, returns the innermost type
   * contained in the array, otherwise will return `ill_formed_type`.
   */
  block_var_type array_contains() const;

  /**
   * Returns number of array dimensions for this type.
   * Returns 0 for non-array types.
   */
  int array_dims() const;

  /**
   * Returns array element type if `var_type_` is `block_array_type`,
   * ill_formed_type otherwise.  (Call `is_array_type()` first.)
   */
  block_var_type array_element_type() const;

  /**
   * Returns array length for block_array_type, nil otherwise.
   */
  expression array_len() const;

  /**
   * Returns vector of array lengths for block_array_type,
   * empty vector otherwise.
   */
  std::vector<expression> array_lens() const;

  /**
   * Returns equivalent bare_expr_type (unsized) for this block type.
   */
  bare_expr_type bare_type() const;

  /**
   * Returns bounds for this type.
   */
  range bounds() const;

  /**
   * Returns true if there are specified upper and/or lower bounds
   * for this type (contained type for arrays), false otherwise.
   */
  bool has_def_bounds() const;

  /**
   * Returns offset and multiplier for this type.
   */
  offset_multiplier ls() const;

  /**
   * Returns true if there are specified offset and/or multiplier
   * for this type (contained type for arrays), false otherwise.
   */
  bool has_def_offset_multiplier() const;

  /**
   * Returns true if `var_type_` is `block_array_type`, false otherwise.
   */
  bool is_array_type() const;

  /**
   * Returns true if `var_type_` has either specified bounds or
   * is a specialized vector or matrix type.
   */
  bool is_constrained() const;

  /**
   * Returns true if `var_type_` is a specialized vector or matrix type.
   * If `var_type_` is array type, evaluates contained type.
   */
  bool is_specialized() const;

  /**
   * If array type, returns innermost type,
   * otherwise returns this type.
   */
  block_var_type innermost_type() const;

  /**
   * Returns Stan language type name.
   */
  std::string name() const;

  /**
   * Returns total number of dimensions for container type.
   * Returns 0 for scalar types.
   */
  int num_dims() const;

  /**
   * Returns an expression for the number of parameters
   * a parameter variable of this type contributes to a model.
   */
  expression params_total() const;
};

/**
 * Stream a user-readable version of the block_var_type to the
 * specified output stream, returning the specified argument
 * output stream to allow chaining.
 *
 * @param o output stream
 * @param x expression type
 * @return argument output stream
 */
std::ostream &operator<<(std::ostream &o, const block_var_type &x);

}  // namespace lang
}  // namespace stan
#endif
