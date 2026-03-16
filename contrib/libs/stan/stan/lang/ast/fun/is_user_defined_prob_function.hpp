#ifndef STAN_LANG_AST_FUN_IS_USER_DEFINED_PROB_FUNCTION_HPP
#define STAN_LANG_AST_FUN_IS_USER_DEFINED_PROB_FUNCTION_HPP

#include <string>
#include <vector>

namespace stan {
  namespace lang {

    struct expression;

    /**
     * Return true if a probability function with the specified name,
     * random variate and parameters is user defined.
     *
     * @param[in] name function name
     * @param[in] variate random variable for probability function
     * @param[in] params parameters to probability function
     */
    bool is_user_defined_prob_function(const std::string& name,
                                       const expression& variate,
                                       const std::vector<expression>& params);

  }
}
#endif
