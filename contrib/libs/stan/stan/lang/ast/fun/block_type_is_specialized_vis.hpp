#ifndef STAN_LANG_AST_FUN_BLOCK_TYPE_IS_SPECIALIZED_VIS_HPP
#define STAN_LANG_AST_FUN_BLOCK_TYPE_IS_SPECIALIZED_VIS_HPP

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
#include <boost/variant/static_visitor.hpp>

namespace stan {
  namespace lang {

    /**
     * Visitor to check for specialized block_var_type, i.e.,
     * wrapped type is not one of int, real, vector, row_vector, matrix, or ill-formed.
     */
    struct block_type_is_specialized_vis : public boost::static_visitor<bool> {
      /**
       * Construct a visitor.
       */
      block_type_is_specialized_vis();

      /**
       * Return true if contained type is specialized.
       *
       * @param x type
       * @return bool
       */
      bool operator()(const block_array_type& x) const;

      /**
       * Return true if type is specialized.
       *
       * @param x type
       * @return bool
       */
      bool operator()(const cholesky_factor_corr_block_type& x) const;

      /**
       * Return true if type is specialized.
       *
       * @param x type
       * @return bool
       */
      bool operator()(const cholesky_factor_cov_block_type& x) const;

      /**
       * Return true if type is specialized.
       *
       * @param x type
       * @return bool
       */
      bool operator()(const corr_matrix_block_type& x) const;

      /**
       * Return true if type is specialized.
       *
       * @param x type
       * @return bool
       */
      bool operator()(const cov_matrix_block_type& x) const;

      /**
       * Return true if type is specialized.
       *
       * @param x type
       * @return bool
       */
      bool operator()(const double_block_type& x) const;

      /**
       * Return true if type is specialized.
       *
       * @param x type
       * @return bool
       */
      bool operator()(const ill_formed_type& x) const;

      /**
       * Return true if type is specialized.
       *
       * @param x type
       * @return bool
       */
      bool operator()(const int_block_type& x) const;

      /**
       * Return true if type is specialized.
       *
       * @param x type
       * @return bool
       */
      bool operator()(const matrix_block_type& x) const;

      /**
       * Return true if type is specialized.
       *
       * @param x type
       * @return bool
       */
      bool operator()(const ordered_block_type& x) const;

      /**
       * Return true if type is specialized.
       *
       * @param x type
       * @return bool
       */
      bool operator()(const positive_ordered_block_type& x) const;

      /**
       * Return true if type is specialized.
       *
       * @param x type
       * @return bool
       */
      bool operator()(const row_vector_block_type& x) const;

      /**
       * Return true if type is specialized.
       *
       * @param x type
       * @return bool
       */
      bool operator()(const simplex_block_type& x) const;

      /**
       * Return true if type is specialized.
       *
       * @param x type
       * @return bool
       */
      bool operator()(const unit_vector_block_type& x) const;

      /**
       * Return true if type is specialized.
       *
       * @param x type
       * @return bool
       */
      bool operator()(const vector_block_type& x) const;
    };
  }
}
#endif
