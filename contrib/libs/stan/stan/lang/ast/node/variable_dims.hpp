#ifndef STAN_LANG_AST_NODE_VARIABLE_DIMS_HPP
#define STAN_LANG_AST_NODE_VARIABLE_DIMS_HPP

#include <string>
#include <vector>

namespace stan {
  namespace lang {

    struct expression;

    /**
     * Structure for holding a variable with its dimension
     * declarations. 
     */
    struct variable_dims {
      /**
       * Name of the variable.
       */
      std::string name_;

      /**
       * Sequence of expressions for dimensions.
       */
      std::vector<expression> dims_;

      /**
       * Construct a default object.
       */
      variable_dims();

      /**
       * Construct with the specified name and dimensions.
       *
       * @param name name of variable
       * @param dims dimensions of variable
       */
      variable_dims(const std::string& name,
                    const std::vector<expression>& dims);
    };

  }
}
#endif
