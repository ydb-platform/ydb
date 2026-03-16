#ifndef STAN_LANG_GENERATOR_GENERATE_INITIALIZER_HPP
#define STAN_LANG_GENERATOR_GENERATE_INITIALIZER_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_bare_type.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate an initializer for a variable of the specified type
     *
     * @param[in] var_type variable type
     * @param[in] scalar_t_name name of scalar type for double values
     * @param[in,out] o stream for generating
     */
    template <typename T>
    void generate_initializer(const T& var_type,
                              const std::string& scalar_t_name,
                              std::ostream& o) {
      std::vector<expression> ar_dim_sizes = var_type.array_lens();
      bare_expr_type bare_type = var_type.array_element_type().bare_type();
      expression arg1 = var_type.arg1();
      expression arg2 = var_type.arg2();
      if (var_type.is_array_type()) {
        arg1 = var_type.array_contains().arg1();
        arg2 = var_type.array_contains().arg2();
      }

      // size of each array dimension (adds open paren)
      for (size_t i = 0; i < ar_dim_sizes.size(); ++i) {
        o << "(";
        generate_expression(ar_dim_sizes[i].expr_, NOT_USER_FACING, o);
        o << ", ";
        generate_bare_type(bare_type, scalar_t_name, o);
        bare_type = bare_type.array_element_type();
      }

      // initialize (array) element
      o << "(";
      if (!is_nil(arg1)) {
        generate_expression(arg1.expr_, NOT_USER_FACING, o);
        if (!is_nil(arg2)) {
          o << ", ";
          generate_expression(arg2.expr_, NOT_USER_FACING, o);
        }
      } else {
        o << "0";
      }
      o << ")";

      // close array parens
      for (size_t i = 0; i < ar_dim_sizes.size(); ++i)
        o << ")";
    }
  }
}
#endif
