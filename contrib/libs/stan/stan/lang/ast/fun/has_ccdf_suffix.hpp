#ifndef STAN_LANG_AST_FUN_HAS_CCDF_SUFFIX_HPP
#define STAN_LANG_AST_FUN_HAS_CCDF_SUFFIX_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Return true if the specified function name has a suffix
     * indicating it is a CCDF.
     *
     * @param[in] name of function
     * @return true if the function has a suffix indicating it is a CCDF
     */
    bool has_ccdf_suffix(const std::string& name);

  }
}
#endif
