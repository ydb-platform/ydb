#ifndef STAN_LANG_AST_DOUBLE_TYPE_HPP
#define STAN_LANG_AST_DOUBLE_TYPE_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Double type.
     */
    struct double_type {
      /**
       * True if variable type declared with "data" qualifier.
       */
      bool is_data_;

      /**
       * Construct a double type with default values.
       */
      double_type();

      /**
       * Construct a double type with the specified data-only variable flag.
       *
       * @param is_data true when var is specified data-only
       */
      explicit double_type(bool is_data);

      /**
       * Returns identity string for this type.
       */
      std::string oid() const;
    };

  }
}
#endif
