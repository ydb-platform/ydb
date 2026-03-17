#ifndef STAN_LANG_AST_FUN_IS_USER_DEFINED_HPP
#define STAN_LANG_AST_FUN_IS_USER_DEFINED_HPP

#include <string>
#include <vector>

namespace stan {
  namespace lang {

    struct expression;
    struct fun;

    /**
     * Return true if the specified function was declared in the
     * functions block.
     *
     * @param[in] fx function with arguments
     */
    bool is_user_defined(const fun& fx);

    /**
     * Return true if a function with the specified name and arguments
     * was defined in the functions block.
     *
     * @param[in] name function name
     * @param[in] args function arguments
     * @return true if function is defined in the functions block
     */
    bool is_user_defined(const std::string& name,
                         const std::vector<expression>& args);

  }
}
#endif
