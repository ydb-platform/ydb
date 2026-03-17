#ifndef STAN_LANG_AST_FUN_HAS_PROB_FUN_SUFFIX_HPP
#define STAN_LANG_AST_FUN_HAS_PROB_FUN_SUFFIX_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Return true if the function with the specified name has a
     * suffix indicating it is a probability function.
     *
     * @param[in] name function name
     * @return true if function anme has a suffix indicating it is a
     * probability function
     */
    bool has_prob_fun_suffix(const std::string& name);

  }
}
#endif
