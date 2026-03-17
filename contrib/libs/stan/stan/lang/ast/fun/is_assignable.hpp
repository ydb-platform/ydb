#ifndef STAN_LANG_AST_FUN_IS_ASSIGNABLE_HPP
#define STAN_LANG_AST_FUN_IS_ASSIGNABLE_HPP

#include <string>
#include <ostream>

namespace stan {
  namespace lang {

    struct bare_expr_type;

    /**
     * Return true if an expression of the right-hand side type is
     * assignable to a variable of the left-hand side type, writing
     * the failure message to the error messages if the asisgnment is
     * not legal.
     *
     * @param[in] l_type type of expression being assigned to
     * @param[in] r_type type of value expression
     * @param[in] failure_message message to write if assignment is
     * not possible
     * @param[in, out] error_msgs stream to which error messages are
     * written 
     * @return true if the assignment is legal
     */
    bool is_assignable(const bare_expr_type& l_type,
                       const bare_expr_type& r_type,
                       const std::string& failure_message,
                       std::ostream& error_msgs);

  }
}
#endif
