#ifndef STAN_LANG_AST_FUN_BLOCK_TYPE_OFFSET_MULTIPLIER_VIS_HPP
#define STAN_LANG_AST_FUN_BLOCK_TYPE_OFFSET_MULTIPLIER_VIS_HPP

#include <boost/variant/static_visitor.hpp>
#include <stan/lang/ast/node/offset_multiplier.hpp>
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
 * Visitor to get offset_multiplier from block_var_type.
 */
struct block_type_offset_multiplier_vis
    : public boost::static_visitor<offset_multiplier> {
  /**
   * Construct a visitor.
   */
  block_type_offset_multiplier_vis();

  /**
   * Return offset_multiplier for this type.
   *
   * @param x type
   * @return offset_multiplier
   */
  offset_multiplier operator()(const block_array_type &x) const;

  /**
   * Return offset_multiplier for this type.
   *
   * @param x type
   * @return offset_multiplier
   */
  offset_multiplier operator()(const cholesky_factor_corr_block_type &x) const;

  /**
   * Return offset_multiplier for this type.
   *
   * @param x type
   * @return offset_multiplier
   */
  offset_multiplier operator()(const cholesky_factor_cov_block_type &x) const;

  /**
   * Return offset_multiplier for this type.
   *
   * @param x type
   * @return offset_multiplier
   */
  offset_multiplier operator()(const corr_matrix_block_type &x) const;

  /**
   * Return offset_multiplier for this type.
   *
   * @param x type
   * @return offset_multiplier
   */
  offset_multiplier operator()(const cov_matrix_block_type &x) const;

  /**
   * Return offset_multiplier for this type.
   *
   * @param x type
   * @return offset_multiplier
   */
  offset_multiplier operator()(const double_block_type &x) const;

  /**
   * Return offset_multiplier for this type.
   *
   * @param x type
   * @return offset_multiplier
   */
  offset_multiplier operator()(const ill_formed_type &x) const;

  /**
   * Return offset_multiplier for this type.
   *
   * @param x type
   * @return offset_multiplier
   */
  offset_multiplier operator()(const int_block_type &x) const;

  /**
   * Return offset_multiplier for this type.
   *
   * @param x type
   * @return offset_multiplier
   */
  offset_multiplier operator()(const matrix_block_type &x) const;

  /**
   * Return offset_multiplier for this type.
   *
   * @param x type
   * @return offset_multiplier
   */
  offset_multiplier operator()(const ordered_block_type &x) const;

  /**
   * Return offset_multiplier for this type.
   *
   * @param x type
   * @return offset_multiplier
   */
  offset_multiplier operator()(const positive_ordered_block_type &x) const;

  /**
   * Return offset_multiplier for this type.
   *
   * @param x type
   * @return offset_multiplier
   */
  offset_multiplier operator()(const row_vector_block_type &x) const;

  /**
   * Return offset_multiplier for this type.
   *
   * @param x type
   * @return offset_multiplier
   */
  offset_multiplier operator()(const simplex_block_type &x) const;

  /**
   * Return offset_multiplier for this type.
   *
   * @param x type
   * @return offset_multiplier
   */
  offset_multiplier operator()(const unit_vector_block_type &x) const;

  /**
   * Return offset_multiplier for this type.
   *
   * @param x type
   * @return offset_multiplier
   */
  offset_multiplier operator()(const vector_block_type &x) const;
};
}  // namespace lang
}  // namespace stan
#endif
