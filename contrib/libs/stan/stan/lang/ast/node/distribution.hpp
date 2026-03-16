#ifndef STAN_LANG_AST_NODE_DISTRIBUTION_HPP
#define STAN_LANG_AST_NODE_DISTRIBUTION_HPP

#include <string>
#include <vector>

namespace stan {
  namespace lang {

    struct expression;

    /**
     * Structure for a distribution with parameters.
     */
    struct distribution {
      /**
       * The name of the distribution.
       */
      std::string family_;

      /**
       * The sequence of parameters for the distribution.
       */
      std::vector<expression> args_;
    };

  }
}
#endif
