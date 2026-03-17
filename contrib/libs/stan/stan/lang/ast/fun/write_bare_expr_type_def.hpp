#ifndef STAN_LANG_AST_FUN_WRITE_BARE_EXPR_TYPE_DEF_HPP
#define STAN_LANG_AST_FUN_WRITE_BARE_EXPR_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>
#include <iostream>

namespace stan {
  namespace lang {

    std::ostream& write_bare_expr_type(std::ostream& o, bare_expr_type type) {
      if (type.array_dims() > 0) {
        int commas = type.array_dims() - 1;
        o << type.array_contains();
        o << "[ ";
        for (int i=0; i < commas; ++i) o << ", ";
        o << "]";
      } else {
        if (type.is_data())
          o << "data ";

        if (type.is_int_type())
          o << "int";
        else if (type.is_double_type())
          o << "real";
        else if (type.is_vector_type())
          o << "vector";
        else if (type.is_row_vector_type())
          o << "row_vector";
        else if (type.is_matrix_type())
          o << "matrix";
        else if (type.is_ill_formed_type())
          o << "ill-formed";
        else if (type.is_void_type())
          o << "void";
        else
          o << "UNKNOWN";
      }
      return o;
    }
  }
}

#endif
