#ifndef STAN_LANG_AST_NODE_INT_LITERAL_HPP
#define STAN_LANG_AST_NODE_INT_LITERAL_HPP

#include <stan/lang/ast/type/bare_expr_type.hpp>
#include <cstddef>

namespace stan {
  namespace lang {

    struct int_literal {
      /**
       * Value of literal.
       */
      int val_;

      /**
       * Expression type of literal.
       */
      bare_expr_type type_;

      /**
       * Construct a default int literal.
       */
      int_literal();

      /**
       * Construct an int literal with the specified value.
       *
       * @param val value of literal
       */
      explicit int_literal(int val);
    };

  }
}
#endif
