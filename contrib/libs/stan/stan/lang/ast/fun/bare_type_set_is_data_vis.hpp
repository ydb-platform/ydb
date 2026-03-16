#ifndef STAN_LANG_AST_FUN_BARE_TYPE_SET_IS_DATA_VIS_HPP
#define STAN_LANG_AST_FUN_BARE_TYPE_SET_IS_DATA_VIS_HPP

#include <stan/lang/ast/type/bare_array_type.hpp>
#include <stan/lang/ast/type/double_type.hpp>
#include <stan/lang/ast/type/ill_formed_type.hpp>
#include <stan/lang/ast/type/int_type.hpp>
#include <stan/lang/ast/type/matrix_type.hpp>
#include <stan/lang/ast/type/row_vector_type.hpp>
#include <stan/lang/ast/type/vector_type.hpp>
#include <boost/variant/static_visitor.hpp>

namespace stan {
  namespace lang {

    /**
     * Visitor to get data restriction status for bare type.
     */
    struct bare_type_set_is_data_vis : public boost::static_visitor<void> {
      /**
       * Construct a visitor.
       */
      bare_type_set_is_data_vis();

      /**
       * Do nothing - bare_array_type elements must be updated.
       *
       * @param x type
       */
      void operator()(bare_array_type& x) const;

      /**
       * Set `is_data_` flag to true.
       *
       * @param x type
       */
      void operator()(double_type& x) const;

      /**
       * Do nothing.
       *
       * @param x type
       */
      void operator()(ill_formed_type& x) const;

      /**
       * Set `is_data_` flag to true.
       *
       * @param x type
       */
      void operator()(int_type& x) const;

      /**
       * Set `is_data_` flag to true.
       *
       * @param x type
       */
      void operator()(matrix_type& x) const;

      /**
       * Set `is_data_` flag to true.
       *
       * @param x type
       */
      void operator()(row_vector_type& x) const;

      /**
       * Set `is_data_` flag to true.
       *
       * @param x type
       */
      void operator()(vector_type& x) const;

      /**
       * Do nothing.
       *
       * @param x type
       */
      void operator()(void_type& x) const;
    };
  }
}
#endif
