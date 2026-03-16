#ifndef STAN_LANG_AST_ROW_VECTOR_TYPE_HPP
#define STAN_LANG_AST_ROW_VECTOR_TYPE_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Row vector type.
     */
    struct row_vector_type {
      /**
       * True if variable type declared with "data" qualifier.
       */
      bool is_data_;

      /**
       * Construct a row vector type with default values.
       */
      row_vector_type();

      /**
       * Construct a row vector type with the specified data-only variable flag.
       *
       * @param is_data true when var is specified data-only
       */
      explicit row_vector_type(bool is_data);

      /**
       * Returns identity string for this type.
       */
      std::string oid() const;
    };

  }
}
#endif
