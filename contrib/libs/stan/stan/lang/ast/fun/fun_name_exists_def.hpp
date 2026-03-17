#ifndef STAN_LANG_AST_FUN_FUN_NAME_EXISTS_DEF_HPP
#define STAN_LANG_AST_FUN_FUN_NAME_EXISTS_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

    bool fun_name_exists(const std::string& name) {
      return function_signatures::instance().has_key(name);
    }

  }
}
#endif
