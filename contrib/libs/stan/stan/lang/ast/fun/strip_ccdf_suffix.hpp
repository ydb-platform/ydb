#ifndef STAN_LANG_AST_FUN_STRIP_CCDF_SUFFIX_HPP
#define STAN_LANG_AST_FUN_STRIP_CCDF_SUFFIX_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Return the result of removing the suffix from the specified
     * function name indicating it is a CCDF.
     *
     * @param[in] dist_fun name of function
     * @return result of removing suffix from function
     */
    std::string strip_ccdf_suffix(const std::string& dist_fun);

  }
}
#endif
