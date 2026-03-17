#ifndef STAN_LANG_AST_FUN_ENDS_WITH_DEF_HPP
#define STAN_LANG_AST_FUN_ENDS_WITH_DEF_HPP

#include <string>

namespace stan {
  namespace lang {

    bool ends_with(const std::string& suffix, const std::string& s) {
      size_t idx = s.rfind(suffix);
      return idx != std::string::npos && idx == (s.size() - suffix.size());
    }

  }
}
#endif
