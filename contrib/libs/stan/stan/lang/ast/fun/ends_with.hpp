#ifndef STAN_LANG_AST_FUN_ENDS_WITH_HPP
#define STAN_LANG_AST_FUN_ENDS_WITH_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Returns true if the specified suffix appears at the end of the
     * specified string.
     *
     * @param suffix suffix to test
     * @param s string in which to search
     * @return true if the string ends with the suffix
     */
    bool ends_with(const std::string& suffix, const std::string& s);

  }
}
#endif
