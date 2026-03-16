#ifndef STAN_LANG_AST_FUN_HAS_CDF_SUFFIX_HPP
#define STAN_LANG_AST_FUN_HAS_CDF_SUFFIX_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Return true if the specified function name has a suffix
     * indicating it is a CDF.
     *
     * @param[in] name of function
     * @return true if the function has a suffix indicating it is a CDF
     */
    bool has_cdf_suffix(const std::string& name);
  }
}
#endif
