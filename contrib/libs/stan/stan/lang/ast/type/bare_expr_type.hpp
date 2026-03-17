#ifndef STAN_LANG_AST_BARE_EXPR_TYPE_HPP
#define STAN_LANG_AST_BARE_EXPR_TYPE_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <string>
#include <ostream>
#include <cstddef>

namespace stan {
namespace lang {

/**
 * Bare type for Stan variables and expressions.
 */
struct bare_array_type;
struct double_type;
struct ill_formed_type;
struct int_type;
struct matrix_type;
struct row_vector_type;
struct vector_type;
struct void_type;

struct bare_expr_type {
  /**
   * Recursive wrapper for bare types.
   */
  typedef boost::variant<boost::recursive_wrapper<ill_formed_type>,
                         boost::recursive_wrapper<double_type>,
                         boost::recursive_wrapper<int_type>,
                         boost::recursive_wrapper<matrix_type>,
                         boost::recursive_wrapper<row_vector_type>,
                         boost::recursive_wrapper<vector_type>,
                         boost::recursive_wrapper<void_type>,
                         boost::recursive_wrapper<bare_array_type> >
      bare_t;

  /**
   * The bare type held by this wrapper.
   */
  bare_t bare_type_;

  /**
   * Construct a bare var type with default values.
   */
  bare_expr_type();

  /**
   * Construct a bare var type with the specified variant type.
   *
   * @param type bare type raw variant type.
   */
  bare_expr_type(const bare_expr_type& type);  // NOLINT(runtime/explicit)

  /**
   * Construct a bare type with the specified type.
   *
   * @param type bare type
   */
  bare_expr_type(const ill_formed_type& type);  // NOLINT(runtime/explicit)

  /**
   * Construct a bare type with the specified type.
   *
   * @param type bare type
   */
  bare_expr_type(const double_type& type);  // NOLINT(runtime/explicit)

  /**
   * Construct a bare type with the specified type.
   *
   * @param type bare type
   */
  bare_expr_type(const int_type& type);  // NOLINT(runtime/explicit)

  /**
   * Construct a bare type with the specified type.
   *
   * @param type bare type
   */
  bare_expr_type(const matrix_type& type);  // NOLINT(runtime/explicit)

  /**
   * Construct a bare type with the specified type.
   *
   * @param type bare type
   */
  bare_expr_type(const row_vector_type& type);  // NOLINT(runtime/explicit)

  /**
   * Construct a bare type with the specified type.
   *
   * @param type bare type
   */
  bare_expr_type(const vector_type& type);  // NOLINT(runtime/explicit)

  /**
   * Construct a bare type with the specified type.
   *
   * @param type bare type
   */
  bare_expr_type(const void_type& type);  // NOLINT(runtime/explicit)

  /**
   * Construct a bare type with the specified type.
   *
   * @param type bare type
   */
  bare_expr_type(const bare_array_type& type);  // NOLINT(runtime/explicit)

  /**
   * Construct a bare type with the specified type.
   *
   * @param type bare type
   */
  bare_expr_type(const bare_t& type);  // NOLINT(runtime/explicit)

  /**
   * Return true if the specified bare type is the same as
   * this bare type.
   *
   * @param bare_type Other bare type.
   * @return result of equality test.
   */
  bool operator==(const bare_expr_type& bare_type) const;

  /**
   * Return true if the specified bare type is not the same as
   * this bare type.
   *
   * @param bare_type Other bare type.
   * @return result of inequality test.
   */
  bool operator!=(const bare_expr_type& bare_type) const;

  /**
   * Return true if this bare type `order_id_`
   * is less than that of the specified bare type.
   *
   * @param bare_type Other bare type.
   * @return result of comparison.
   */
  bool operator<(const bare_expr_type& bare_type) const;

  /**
   * Return true if this bare type `order_id_`
   * is less than or equal to that of the specified bare type.
   *
   * @param bare_type Other bare type.
   * @return result of comparison.
   */
  bool operator<=(const bare_expr_type& bare_type) const;

  /**
   * Return true if this bare type `order_id_`
   * is greater than that of the specified bare type.
   *
   * @param bare_type Other bare type.
   * @return result of comparison.
   */
  bool operator>(const bare_expr_type& bare_type) const;

  /**
   * Return true if this bare type `order_id_`
   * is greater than or equal to that of the specified bare type.
   *
   * @param bare_type Other bare type.
   * @return result of comparison.
   */
  bool operator>=(const bare_expr_type& bare_type) const;

  /**
   * Returns the element type for `bare_array_type`, otherwise
   * will return `ill_formed_type`.
   */
  bare_expr_type array_element_type() const;

  /**
   * If `bare_type` is `bare_array_type`, returns the innermost type
   * contained in the array, otherwise will return `ill_formed_type`.
   */
  bare_expr_type array_contains() const;

  /**
   * Returns number of array dimensions for this type.
   * Returns 0 for non-array types.
   */
  int array_dims() const;

  /**
   * If array type, returns bare_expr_type of innermost type,
   * otherwise returns this type.
   */
  bare_expr_type innermost_type() const;

  /**
   * Returns true if `bare_type_` is `bare_array_type`, false otherwise.
   */
  bool is_array_type() const;

  /**
   * Returns value of `bare_type_` member var `is_data_`.
   */
  bool is_data() const;

  /**
   * Returns true if `bare_type_` is `double_type`, false otherwise.
   */
  bool is_double_type() const;

  /**
   * Returns true if `bare_type_` is `ill_formed_type`, false otherwise.
   */
  bool is_ill_formed_type() const;

  /**
   * Returns true if `bare_type_` is `int_type`, false otherwise.
   */
  bool is_int_type() const;

  /**
   * Returns true if `bare_type_` is `matrix_type`, false otherwise.
   */
  bool is_matrix_type() const;

  /**
   * Returns true if `bare_type_` is `int_type` or `double_type`, false
   * otherwise.
   */
  bool is_primitive() const;

  /**
   * Returns true if `bare_type_` is `row_vector_type`, false otherwise.
   */
  bool is_row_vector_type() const;

  /**
   * Returns true if `bare_type_` is `vector_type`, false otherwise.
   */
  bool is_vector_type() const;

  /**
   * Returns true if `bare_type_` is `void_type`, false otherwise.
   */
  bool is_void_type() const;

  /**
   * Returns total number of dimensions for container type.
   * Returns 0 for scalar types.
   */
  int num_dims() const;

  /**
   * Returns order id for this bare type.
   */
  std::string order_id() const;

  /**
   * Set flag `is_data` to true
   */
  void set_is_data();
};

/**
 * Stream a user-readable version of the bare_expr_type to the
 * specified output stream, returning the specified argument
 * output stream to allow chaining.
 *
 * @param o output stream
 * @param x expression type
 * @return argument output stream
 */
std::ostream& operator<<(std::ostream& o, const bare_expr_type& x);
}  // namespace lang
}  // namespace stan
#endif
