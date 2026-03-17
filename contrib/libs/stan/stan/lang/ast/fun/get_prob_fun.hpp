#ifndef STAN_LANG_AST_FUN_GET_PROB_FUN_HPP
#define STAN_LANG_AST_FUN_GET_PROB_FUN_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Return the probability function (density or mass) for the
     * specified distribution name.
     *
     * @param[in] dist_name name of distribution
     * @return probability function for distribution
     */
    std::string get_prob_fun(const std::string& dist_name);

  }
}
#endif
