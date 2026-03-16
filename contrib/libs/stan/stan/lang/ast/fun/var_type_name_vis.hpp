#ifndef STAN_LANG_AST_FUN_VAR_TYPE_NAME_VIS_HPP
#define STAN_LANG_AST_FUN_VAR_TYPE_NAME_VIS_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <stan/lang/ast/type/block_array_type.hpp>
#include <stan/lang/ast/type/local_array_type.hpp>
#include <stan/lang/ast/type/cholesky_factor_corr_block_type.hpp>
#include <stan/lang/ast/type/cholesky_factor_cov_block_type.hpp>
#include <stan/lang/ast/type/corr_matrix_block_type.hpp>
#include <stan/lang/ast/type/cov_matrix_block_type.hpp>
#include <stan/lang/ast/type/double_block_type.hpp>
#include <stan/lang/ast/type/double_type.hpp>
#include <stan/lang/ast/type/ill_formed_type.hpp>
#include <stan/lang/ast/type/int_block_type.hpp>
#include <stan/lang/ast/type/int_type.hpp>
#include <stan/lang/ast/type/matrix_block_type.hpp>
#include <stan/lang/ast/type/matrix_local_type.hpp>
#include <stan/lang/ast/type/ordered_block_type.hpp>
#include <stan/lang/ast/type/positive_ordered_block_type.hpp>
#include <stan/lang/ast/type/row_vector_block_type.hpp>
#include <stan/lang/ast/type/row_vector_local_type.hpp>
#include <stan/lang/ast/type/simplex_block_type.hpp>
#include <stan/lang/ast/type/unit_vector_block_type.hpp>
#include <stan/lang/ast/type/vector_block_type.hpp>
#include <stan/lang/ast/type/vector_local_type.hpp>
#include <boost/variant/static_visitor.hpp>
#include <vector>
#include <string>

namespace stan {
namespace lang {
/**
 * Visitor to Stan language name for type.
 */
struct var_type_name_vis : public boost::static_visitor<std::string> {
  /**
   * Construct a visitor.
   */
  var_type_name_vis();

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const block_array_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const local_array_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const cholesky_factor_corr_block_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const cholesky_factor_cov_block_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const corr_matrix_block_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const cov_matrix_block_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const double_block_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const double_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const ill_formed_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const int_block_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const int_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const matrix_block_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const matrix_local_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const ordered_block_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const positive_ordered_block_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const row_vector_block_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const row_vector_local_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const simplex_block_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const unit_vector_block_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const vector_block_type& x) const;

  /**
   * Return name for this type.
   *
   * @param x type
   * @return name
   */
  std::string operator()(const vector_local_type& x) const;
};
}  // namespace lang
}  // namespace stan
#endif
