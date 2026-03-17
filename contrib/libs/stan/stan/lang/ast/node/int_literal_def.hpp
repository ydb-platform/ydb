#ifndef STAN_LANG_AST_NODE_INT_LITERAL_DEF_HPP
#define STAN_LANG_AST_NODE_INT_LITERAL_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    int_literal::int_literal() : type_(int_type()) { }

    int_literal::int_literal(int val) : val_(val),  type_(int_type()) { }

  }
}
#endif
