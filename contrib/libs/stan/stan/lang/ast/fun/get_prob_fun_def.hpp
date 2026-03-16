#ifndef STAN_LANG_AST_FUN_GET_PROB_FUN_DEF_HPP
#define STAN_LANG_AST_FUN_GET_PROB_FUN_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

    std::string get_prob_fun(const std::string& dist_name) {
      if (function_signatures::instance().has_key(dist_name + "_log"))
        return dist_name + "_log";
      else if (function_signatures::instance().has_key(dist_name + "_lpdf"))
        return dist_name + "_lpdf";
      else if (function_signatures::instance().has_key(dist_name + "_lpmf"))
        return dist_name + "_lpmf";
      else
        return dist_name;
    }

  }
}
#endif
