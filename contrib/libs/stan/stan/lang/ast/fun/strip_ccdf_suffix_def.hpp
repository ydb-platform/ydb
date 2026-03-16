#ifndef STAN_LANG_AST_FUN_STRIP_CCDF_SUFFIX_DEF_HPP
#define STAN_LANG_AST_FUN_STRIP_CCDF_SUFFIX_DEF_HPP

#include <stan/lang/ast/fun/ends_with.hpp>
#include <string>

namespace stan {
namespace lang {

std::string strip_ccdf_suffix(const std::string& fname) {
  if (ends_with("_lccdf", fname))
    return fname.substr(0, fname.size() - 6);
  else if (ends_with("_ccdf_log", fname))
    return fname.substr(0, fname.size() - 9);
  else
    return fname;
}

}  // namespace lang
}  // namespace stan
#endif
