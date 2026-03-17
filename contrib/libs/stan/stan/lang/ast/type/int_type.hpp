#ifndef STAN_LANG_AST_INT_TYPE_HPP
#define STAN_LANG_AST_INT_TYPE_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Integer type.
     */
    struct int_type {
      /**
       * True if variable type declared with "data" qualifier.
       */
      bool is_data_;

      /**
       * Construct a int type with default values.
       */
      int_type();

      /**
       * Construct a int type with the specified data-only variable flag.
       *
       * @param is_data true when var is specified data-only
       */
      explicit int_type(bool is_data);


      /**
       * Returns identity string for this type.
       */
      std::string oid() const;
    };

  }
}
#endif
