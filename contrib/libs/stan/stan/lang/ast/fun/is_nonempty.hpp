#ifndef STAN_LANG_AST_FUN_IS_NONEMPTY_HPP
#define STAN_LANG_AST_FUN_IS_NONEMPTY_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Returns true if the specified string contains a character other
     * than a whitespace character.
     *
     * @param s string to test
     * @return true if string contains a non-space character
     */
    bool is_nonempty(const std::string& s);

  }
}
#endif
