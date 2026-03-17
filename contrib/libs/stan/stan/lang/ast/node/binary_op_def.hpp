#ifndef STAN_LANG_AST_NODE_BINARY_OP_DEF_HPP
#define STAN_LANG_AST_NODE_BINARY_OP_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

    binary_op::binary_op() { }

    binary_op::binary_op(const expression& left, const std::string& op,
                         const expression& right)
      : op(op), left(left), right(right),
        type_(promote_primitive(left.bare_type(),
                                right.bare_type())) {
    }

  }
}
#endif
