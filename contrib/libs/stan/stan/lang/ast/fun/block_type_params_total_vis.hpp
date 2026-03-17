#ifndef STAN_LANG_AST_FUN_BLOCK_TYPE_PARAMS_TOTAL_VIS_HPP
#define STAN_LANG_AST_FUN_BLOCK_TYPE_PARAMS_TOTAL_VIS_HPP

#include <stan/lang/ast/node/range.hpp>
#include <stan/lang/ast/type/bare_expr_type.hpp>
#include <stan/lang/ast/type/block_array_type.hpp>
#include <stan/lang/ast/type/cholesky_factor_corr_block_type.hpp>
#include <stan/lang/ast/type/cholesky_factor_cov_block_type.hpp>
#include <stan/lang/ast/type/corr_matrix_block_type.hpp>
#include <stan/lang/ast/type/cov_matrix_block_type.hpp>
#include <stan/lang/ast/type/double_block_type.hpp>
#include <stan/lang/ast/type/ill_formed_type.hpp>
#include <stan/lang/ast/type/int_block_type.hpp>
#include <stan/lang/ast/type/matrix_block_type.hpp>
#include <stan/lang/ast/type/ordered_block_type.hpp>
#include <stan/lang/ast/type/positive_ordered_block_type.hpp>
#include <stan/lang/ast/type/row_vector_block_type.hpp>
#include <stan/lang/ast/type/simplex_block_type.hpp>
#include <stan/lang/ast/type/unit_vector_block_type.hpp>
#include <stan/lang/ast/type/vector_block_type.hpp>

namespace stan {
namespace lang {

/**
 * Visitor to build an expression for the total number of parameters
 * a parameter variable of this type contributes to a model.
 */
struct block_type_params_total_vis : public boost::static_visitor<expression> {
  /**
   * Construct a visitor.
   */
  block_type_params_total_vis();

  /**
   * Return an expression for the number of parameters for this type.
   *
   * @param x type
   * @return expression
   */
  expression operator()(const block_array_type& x) const;

  /**
   * Return an expression for the number of parameters for this type.
   *
   * @param x type
   * @return expression
   */
  expression operator()(const cholesky_factor_corr_block_type& x) const;

  /**
   * Return an expression for the number of parameters for this type.
   *
   * @param x type
   * @return expression
   */
  expression operator()(const cholesky_factor_cov_block_type& x) const;

  /**
   * Return an expression for the number of parameters for this type.
   *
   * @param x type
   * @return expression
   */
  expression operator()(const corr_matrix_block_type& x) const;

  /**
   * Return an expression for the number of parameters for this type.
   *
   * @param x type
   * @return expression
   */
  expression operator()(const cov_matrix_block_type& x) const;

  /**
   * Return an expression for the number of parameters for this type.
   *
   * @param x type
   * @return expression
   */
  expression operator()(const double_block_type& x) const;

  /**
   * Return an expression for the number of parameters for this type.
   *
   * @param x type
   * @return expression
   */
  expression operator()(const ill_formed_type& x) const;

  /**
   * Return an expression for the number of parameters for this type.
   *
   * @param x type
   * @return expression
   */
  expression operator()(const int_block_type& x) const;

  /**
   * Return an expression for the number of parameters for this type.
   *
   * @param x type
   * @return expression
   */
  expression operator()(const matrix_block_type& x) const;

  /**
   * Return an expression for the number of parameters for this type.
   *
   * @param x type
   * @return expression
   */
  expression operator()(const ordered_block_type& x) const;

  /**
   * Return an expression for the number of parameters for this type.
   *
   * @param x type
   * @return expression
   */
  expression operator()(const positive_ordered_block_type& x) const;

  /**
   * Return an expression for the number of parameters for this type.
   *
   * @param x type
   * @return expression
   */
  expression operator()(const row_vector_block_type& x) const;

  /**
   * Return an expression for the number of parameters for this type.
   *
   * @param x type
   * @return expression
   */
  expression operator()(const simplex_block_type& x) const;

  /**
   * Return an expression for the number of parameters for this type.
   *
   * @param x type
   * @return expression
   */
  expression operator()(const unit_vector_block_type& x) const;

  /**
   * Return an expression for the number of parameters for this type.
   *
   * @param x type
   * @return expression
   */
  expression operator()(const vector_block_type& x) const;
};
}  // namespace lang
}  // namespace stan
#endif
