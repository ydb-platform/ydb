#ifndef STAN_LANG_AST_NODE_VARIABLE_HPP
#define STAN_LANG_AST_NODE_VARIABLE_HPP

#include <stan/lang/ast/type/bare_expr_type.hpp>
#include <cstddef>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Structure to hold a variable.
     */
    struct variable {
      /**
       * Name of variable.
       */
      std::string name_;

      /**
       * Type of variable.
       */
      bare_expr_type type_;

      /**
       * Construct a default variable.
       */
      variable();

      /**
       * Construct a variable with the specified name and nil type.
       *
       * @param name variable name
       */
      variable(const std::string& name);  // NOLINT(runtime/explicit)

      /**
       * Set the variable type.
       *
       * @param bare_type bare expression type
       */
      void set_type(const bare_expr_type& bare_type);
    };

  }
}
#endif
