#ifndef STAN_LANG_AST_FUN_IS_ASSIGNABLE_DEF_HPP
#define STAN_LANG_AST_FUN_IS_ASSIGNABLE_DEF_HPP

#include <stan/lang/ast.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    bool is_assignable(const bare_expr_type& l_type,
                       const bare_expr_type& r_type,
                       const std::string& failure_message,
                       std::ostream& error_msgs) {
      bool assignable = true;
      if (l_type.num_dims() != r_type.num_dims()) {
        assignable = false;
        error_msgs << "Mismatched array dimensions.";
      }
      if (!(l_type == r_type
            || (l_type.is_double_type()
                && r_type.is_int_type()))) {
        assignable = false;
        error_msgs << "Base type mismatch. ";
      }
      if (!assignable)
        error_msgs << failure_message << std::endl
                   << "    LHS type = " << l_type << "; RHS type = " << r_type
                   << std::endl;
      return assignable;
    }

  }
}
#endif
