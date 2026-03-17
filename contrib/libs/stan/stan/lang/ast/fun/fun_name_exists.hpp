#ifndef STAN_LANG_AST_FUN_FUN_NAME_EXISTS_HPP
#define STAN_LANG_AST_FUN_FUN_NAME_EXISTS_HPP

#include <string>

namespace stan {
  namespace lang {

    /**
     * Return true if the function name has been declared as a
     * built-in or by the user.  
     *
     * @param name name of function
     * @return true if it has been declared
     */
    bool fun_name_exists(const std::string& name);

  }
}
#endif
