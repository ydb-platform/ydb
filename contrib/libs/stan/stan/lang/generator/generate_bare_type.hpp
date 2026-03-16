#ifndef STAN_LANG_GENERATOR_GENERATE_BARE_TYPE_HPP
#define STAN_LANG_GENERATOR_GENERATE_BARE_TYPE_HPP

#include <stan/lang/ast.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {


    /**
     * Generate the basic type for the specified expression type
     * using the specified scalar type string and writing to
     * the specified stream.
     *
     * Scalar type string is `local_scalar_t__` in log_prob method,
     * `double` elsewhere.
     *
     * @param[in] t expression type
     * @param[in] scalar_t_name name of scalar type for double values
     * @param[in] o stream for generating
     */
    void generate_bare_type(const bare_expr_type& t,
                            const std::string& scalar_t_name,
                            std::ostream& o) {
      for (int i = 0; i < t.array_dims(); ++i)
        o << "std::vector<";
      bool is_template_type = false;
      if (t.innermost_type().is_int_type()) {
        o << "int";
        is_template_type = false;
      } else if (t.innermost_type().is_double_type()) {
        o << scalar_t_name;
        is_template_type = false;
      } else if (t.innermost_type().is_vector_type()) {
        o << "Eigen::Matrix<"
          << scalar_t_name
          << ", Eigen::Dynamic, 1>";
        is_template_type = true;
      } else if (t.innermost_type().is_row_vector_type()) {
        o << "Eigen::Matrix<"
          << scalar_t_name
          << ", 1, Eigen::Dynamic>";
        is_template_type = true;
      } else if (t.innermost_type().is_matrix_type()) {
        o << "Eigen::Matrix<"
          << scalar_t_name
          << ", Eigen::Dynamic, Eigen::Dynamic>";
        is_template_type = true;
      } else if (t.innermost_type().is_void_type()) {
        o << "void";
      } else {
        o << "UNKNOWN TYPE";
      }
      for (int i = 0; i < t.array_dims(); ++i) {
        if (i > 0 || is_template_type)
          o << ' ';
        o << '>';
      }
    }

  }
}
#endif
