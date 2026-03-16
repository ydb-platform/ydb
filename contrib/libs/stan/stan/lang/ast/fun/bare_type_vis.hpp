#ifndef STAN_LANG_AST_FUN_BARE_TYPE_VIS_HPP
#define STAN_LANG_AST_FUN_BARE_TYPE_VIS_HPP

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

namespace stan {
  namespace lang {

    /**
     * Visitor to get bare type for local and block var types.
     */
    struct bare_type_vis : public boost::static_visitor<bare_expr_type> {
      /**
       * Construct a visitor.
       */
      bare_type_vis();

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const block_array_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const local_array_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const cholesky_factor_corr_block_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const cholesky_factor_cov_block_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const corr_matrix_block_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const cov_matrix_block_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const double_block_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const double_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const ill_formed_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const int_block_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const int_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const matrix_block_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const matrix_local_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const ordered_block_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const positive_ordered_block_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const row_vector_block_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const row_vector_local_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const simplex_block_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const unit_vector_block_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const vector_block_type& x) const;

      /**
       * Return equivalent bare type.
       *
       * @param x type
       * @return bare type
       */
      bare_expr_type operator()(const vector_local_type& x) const;
    };
  }
}
#endif
