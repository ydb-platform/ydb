#ifndef STAN_LANG_GENERATOR_GET_VERBOSE_VAR_TYPE_HPP
#define STAN_LANG_GENERATOR_GET_VERBOSE_VAR_TYPE_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {
    /**
     * Return type string for variable type.
     *
     * @param[in] bare_type expression type
     */
    std::string
    get_verbose_var_type(const bare_expr_type bare_type) {
      if (bare_type.innermost_type().is_matrix_type()) {
        return
          "Eigen::Matrix<local_scalar_t__, Eigen::Dynamic, Eigen::Dynamic>";
      } else if (bare_type.innermost_type().is_row_vector_type()) {
        return "Eigen::Matrix<local_scalar_t__, 1, Eigen::Dynamic>";
      } else if (bare_type.innermost_type().is_vector_type()) {
        return "Eigen::Matrix<local_scalar_t__, Eigen::Dynamic, 1>";
      } else if (bare_type.innermost_type().is_double_type()) {
        return "local_scalar_t__";
      } else if (bare_type.innermost_type().is_int_type()) {
        return "int";
      }
      return "ill_formed";
    }
  }
}
#endif
