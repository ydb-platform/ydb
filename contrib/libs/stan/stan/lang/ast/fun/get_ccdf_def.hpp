#ifndef STAN_LANG_AST_FUN_GET_CCDF_DEF_HPP
#define STAN_LANG_AST_FUN_GET_CCDF_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

    std::string get_ccdf(const std::string& dist_name) {
      if (function_signatures::instance().has_key(dist_name + "_ccdf_log"))
        return dist_name + "_ccdf_log";
      else if (function_signatures::instance().has_key(dist_name + "_lccdf"))
        return dist_name + "_lccdf";
      else
        return dist_name;
    }

  }
}
#endif
