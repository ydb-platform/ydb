#ifndef STAN_LANG_AST_FUN_GET_CCDF_HPP
#define STAN_LANG_AST_FUN_GET_CCDF_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Return the CCDF for the specified distribution.
     *
     * @param[in] dist_name name of distribution
     * @return CCDF for distribution
     */
    std::string get_ccdf(const std::string& dist_name);

  }
}
#endif
