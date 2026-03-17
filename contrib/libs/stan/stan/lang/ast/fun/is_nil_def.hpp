#ifndef STAN_LANG_AST_FUN_IS_NIL_DEF_HPP
#define STAN_LANG_AST_FUN_IS_NIL_DEF_HPP

#include <stan/lang/ast.hpp>
#include <boost/variant/apply_visitor.hpp>

namespace stan {
  namespace lang {

    bool is_nil(const expression& e) {
      is_nil_vis ino;
      return boost::apply_visitor(ino, e.expr_);
    }

  }
}
#endif
