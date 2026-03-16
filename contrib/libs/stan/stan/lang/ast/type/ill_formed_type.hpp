#ifndef STAN_LANG_AST_ILL_FORMED_TYPE_HPP
#define STAN_LANG_AST_ILL_FORMED_TYPE_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Ill_Formed type.
     */
    struct ill_formed_type {
      /**
       * True if variable type declared with "data" qualifier.
       * Always false.
       */
      bool is_data_;

      /**
       * Construct an ill_formed type with default values.
       */
      ill_formed_type();

      /**
       * Returns identity string for this type.
       */
      std::string oid() const;
    };

  }
}
#endif

