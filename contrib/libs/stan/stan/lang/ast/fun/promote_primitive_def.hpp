#ifndef STAN_LANG_AST_FUN_PROMOTE_PRIMITIVE_DEF_HPP
#define STAN_LANG_AST_FUN_PROMOTE_PRIMITIVE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

   bare_expr_type promote_primitive(const bare_expr_type& et) {
      if (!et.is_primitive())
        return ill_formed_type();
      return et;
    }

    bare_expr_type promote_primitive(const bare_expr_type& et1,
                                     const bare_expr_type& et2) {
      if (!et1.is_primitive() || !et2.is_primitive())
        return ill_formed_type();
      return et1.is_double_type() ? et1 : et2;
    }

  }
}
#endif
