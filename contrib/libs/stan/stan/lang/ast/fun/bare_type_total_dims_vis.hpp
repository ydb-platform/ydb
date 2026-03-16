#ifndef STAN_LANG_AST_FUN_BARE_TYPE_TOTAL_DIMS_VIS_HPP
#define STAN_LANG_AST_FUN_BARE_TYPE_TOTAL_DIMS_VIS_HPP

#include <stan/lang/ast/type/bare_array_type.hpp>
#include <stan/lang/ast/type/double_type.hpp>
#include <stan/lang/ast/type/ill_formed_type.hpp>
#include <stan/lang/ast/type/int_type.hpp>
#include <stan/lang/ast/type/matrix_type.hpp>
#include <stan/lang/ast/type/row_vector_type.hpp>
#include <stan/lang/ast/type/vector_type.hpp>
#include <stan/lang/ast/type/void_type.hpp>
#include <boost/variant/static_visitor.hpp>

namespace stan {
  namespace lang {
    /**
     * Visitor to count total number of dimensions for a bare_expr_type.
     * Total is array dimensions and +1 for vectors or +2 for matrices.
     */
    struct bare_type_total_dims_vis : public boost::static_visitor<int> {
      /**
       * Construct a bare_type_total_dims visitor.
       */
      bare_type_total_dims_vis();

      /**
       * Returns total number of dimensions.
       *
       * @param x type
       */
      int operator()(const bare_array_type& x) const;

      /**
       * Returns total number of dimensions.
       *
       * @param x type
       */
      int operator()(const double_type& x) const;

      /**
       * Returns total number of dimensions.
       *
       * @param x type
       */
      int operator()(const ill_formed_type& x) const;

      /**
       * Returns total number of dimensions.
       *
       * @param x type
       */
      int operator()(const int_type& x) const;

      /**
       * Returns total number of dimensions.
       *
       * @param x type
       */
      int operator()(const matrix_type& x) const;

      /**
       * Returns total number of dimensions.
       *
       * @param x type
       */
      int operator()(const row_vector_type& x) const;

      /**
       * Returns total number of dimensions.
       *
       * @param x type
       */
      int operator()(const vector_type& x) const;

      /**
       * Returns total number of dimensions.
       *
       * @param x type
       */
      int operator()(const void_type& x) const;
    };
  }
}
#endif
