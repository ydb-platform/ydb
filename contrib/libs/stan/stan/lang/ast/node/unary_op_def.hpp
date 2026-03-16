#ifndef STAN_LANG_AST_NODE_UNARY_OP_DEF_HPP
#define STAN_LANG_AST_NODE_UNARY_OP_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    unary_op::unary_op() { }

    unary_op::unary_op(char op, const expression& subject)
      : op(op), subject(subject),
        type_(promote_primitive(subject.bare_type())) {
    }

  }
}
#endif


