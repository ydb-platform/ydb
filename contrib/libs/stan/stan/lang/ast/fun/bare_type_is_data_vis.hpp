#ifndef STAN_LANG_AST_FUN_BARE_TYPE_IS_DATA_VIS_HPP
#define STAN_LANG_AST_FUN_BARE_TYPE_IS_DATA_VIS_HPP

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
    struct bare_type_is_data_vis : public boost::static_visitor<bool> {
      /**
       * Construct a visitor.
       */
      bare_type_is_data_vis();

      /**
       * Return variable restriction status for this type.
       *
       * @param x type
       * @return is_data flag
       */
      bool operator()(const bare_array_type& x) const;

      /**
       * Return variable restriction status for this type.
       *
       * @param x type
       * @return is_data flag
       */
      bool operator()(const double_type& x) const;

      /**
       * Return variable restriction status for this type.
       *
       * @param x type
       * @return is_data flag
       */
      bool operator()(const ill_formed_type& x) const;

      /**
       * Return variable restriction status for this type.
       *
       * @param x type
       * @return is_data flag
       */
      bool operator()(const int_type& x) const;

      /**
       * Return variable restriction status for this type.
       *
       * @param x type
       * @return is_data flag
       */
      bool operator()(const matrix_type& x) const;

      /**
       * Return variable restriction status for this type.
       *
       * @param x type
       * @return is_data flag
       */
      bool operator()(const row_vector_type& x) const;

      /**
       * Return variable restriction status for this type.
       *
       * @param x type
       * @return is_data flag
       */
      bool operator()(const vector_type& x) const;

      /**
       * Return variable restriction status for this type.
       *
       * @param x type
       * @return is_data flag
       */
      bool operator()(const void_type& x) const;
    };
  }
}
#endif
