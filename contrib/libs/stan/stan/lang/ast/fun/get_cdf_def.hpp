#ifndef STAN_LANG_AST_FUN_GET_CDF_DEF_HPP
#define STAN_LANG_AST_FUN_GET_CDF_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

   std::string get_cdf(const std::string& dist_name) {
      if (function_signatures::instance().has_key(dist_name + "_cdf_log"))
        return dist_name + "_cdf_log";
      else if (function_signatures::instance().has_key(dist_name + "_lcdf"))
        return dist_name + "_lcdf";
      else
        return dist_name;
    }

  }
}
#endif
