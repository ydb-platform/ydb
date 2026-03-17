#ifndef STAN_LANG_AST_FUN_GET_CDF_HPP
#define STAN_LANG_AST_FUN_GET_CDF_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Return the name of the CDF for the specified distribution name.
     *
     * @param dist_name name of distribution
     * @return name of CDF 
     */
    std::string get_cdf(const std::string& dist_name);

  }
}
#endif
