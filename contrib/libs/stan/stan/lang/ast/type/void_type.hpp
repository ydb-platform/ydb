#ifndef STAN_LANG_AST_VOID_TYPE_HPP
#define STAN_LANG_AST_VOID_TYPE_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Void type.
     */
    struct void_type {
      /**
       * True if variable type declared with "data" qualifier.
       * Always false.
       */
      bool is_data_;

      /**
       * Construct a void type with default values.
       */
      void_type();

      /**
       * Returns identity string for this type.
       */
      std::string oid() const;
    };

  }
}
#endif
