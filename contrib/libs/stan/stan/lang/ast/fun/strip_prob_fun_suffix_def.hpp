#ifndef STAN_LANG_AST_FUN_STRIP_PROB_FUN_SUFFIX_DEF_HPP
#define STAN_LANG_AST_FUN_STRIP_PROB_FUN_SUFFIX_DEF_HPP

#include <stan/lang/ast/fun/ends_with.hpp>
#include <string>

namespace stan {
namespace lang {

std::string strip_prob_fun_suffix(const std::string& fname) {
  if (ends_with("_lpdf", fname))
    return fname.substr(0, fname.size() - 5);
  else if (ends_with("_lpmf", fname))
    return fname.substr(0, fname.size() - 5);
  else if (ends_with("_log", fname))
    return fname.substr(0, fname.size() - 4);
  else
    return fname;
}

}  // namespace lang
}  // namespace stan
#endif
