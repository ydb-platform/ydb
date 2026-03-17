#ifndef STAN_LANG_AST_FUN_IS_USER_DEFINED_PROB_FUNCTION_DEF_HPP
#define STAN_LANG_AST_FUN_IS_USER_DEFINED_PROB_FUNCTION_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    bool is_user_defined_prob_function(const std::string& name,
                                       const expression& variate,
                                       const std::vector<expression>& params) {
      std::vector<expression> variate_params;
      variate_params.push_back(variate);
      for (size_t i = 0; i < params.size(); ++i)
        variate_params.push_back(params[i]);
      return is_user_defined(name, variate_params);
    }

  }
}
#endif
