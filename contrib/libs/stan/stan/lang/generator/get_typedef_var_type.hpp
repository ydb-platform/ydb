#ifndef STAN_LANG_GENERATOR_GET_TYPEDEF_VAR_TYPE_HPP
#define STAN_LANG_GENERATOR_GET_TYPEDEF_VAR_TYPE_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {
    /**
     * Return cpp type name or stan_math typedef used for bare_expr_type.
     *
     * @param[in] bare_type bare_type
     */
    std::string
    get_typedef_var_type(const bare_expr_type& bare_type) {
      if (bare_type.innermost_type().is_matrix_type()) {
        return "matrix_d";
      } else if (bare_type.innermost_type().is_row_vector_type()) {
        return "row_vector_d";
      } else if (bare_type.innermost_type().is_vector_type()) {
        return "vector_d";
      } else if (bare_type.innermost_type().is_double_type()) {
        return "double";
      } else if (bare_type.innermost_type().is_int_type()) {
        return "int";
      }
      return "ill_formed";
    }
  }
}
#endif
