#ifndef STAN_LANG_AST_NODE_CONDITIONAL_OP_DEF_HPP
#define STAN_LANG_AST_NODE_CONDITIONAL_OP_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    conditional_op::conditional_op()
      : has_var_(false) { }

    conditional_op::conditional_op(const expression& cond,
                                   const expression& true_val,
                                   const expression& false_val)
      : cond_(cond), true_val_(true_val), false_val_(false_val),
        type_(promote_primitive(true_val.bare_type(),
                                false_val.bare_type())),
        has_var_(false), scope_() {
    }

  }
}
#endif
