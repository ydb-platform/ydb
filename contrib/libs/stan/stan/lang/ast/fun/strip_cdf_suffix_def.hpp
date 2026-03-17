#ifndef STAN_LANG_AST_FUN_STRIP_CDF_SUFFIX_DEF_HPP
#define STAN_LANG_AST_FUN_STRIP_CDF_SUFFIX_DEF_HPP

#include <stan/lang/ast/fun/ends_with.hpp>
#include <string>

namespace stan {
namespace lang {

std::string strip_cdf_suffix(const std::string& fname) {
  if (ends_with("_lcdf", fname))
    return fname.substr(0, fname.size() - 5);
  else if (ends_with("_cdf_log", fname))
    return fname.substr(0, fname.size() - 8);
  else
    return fname;
}

}  // namespace lang
}  // namespace stan
#endif
