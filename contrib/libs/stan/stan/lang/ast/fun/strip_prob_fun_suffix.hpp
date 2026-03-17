#ifndef STAN_LANG_AST_FUN_STRIP_PROB_FUN_SUFFIX_HPP
#define STAN_LANG_AST_FUN_STRIP_PROB_FUN_SUFFIX_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Return the result of stripping the suffix indicating it is a
     * probability function from the specified function name.
     *
     * @param[in] dist_fun name of probability function
     * @return the probability function with the suffix stripped off
     */
    std::string strip_prob_fun_suffix(const std::string& dist_fun);

  }
}
#endif
