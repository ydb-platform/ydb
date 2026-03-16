#ifndef STAN_LANG_GENERATOR_WRITE_VAR_DECL_ARG_HPP
#define STAN_LANG_GENERATOR_WRITE_VAR_DECL_ARG_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_expression.hpp>
#include <stan/lang/generator/get_verbose_var_type.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {
    /**
     * Write the initial value passed as an argument to the
     * variable declaration constructor.
     * For int type, initial value is 0.
     * For double type, initial value is DUMMY_VAR
     * For container types, generate proper set of nested types.
     *
     * Note:  this is called after array type has been unfolded,
     * so bare_type shouldn't be bare_array_type (or ill_formed_type).
     *
     * @param[in] bare_type
     * @param[in] cpp_type_str generated cpp type
     * @param[in] ar_lens vector of sizes for each array dimension
     * @param[in] arg1 expression for size of first dim of vec/matrix (or nil)
     * @param[in] arg2 expression for size of second dim of matrix (or nil)
     * @param[in,out] o stream for generating
     */
    void
    write_var_decl_arg(const bare_expr_type& bare_type,
                       const std::string& cpp_type_str,
                       const std::vector<expression>& ar_lens,
                       const expression& arg1,
                       const expression& arg2,
                       std::ostream& o) {
      bool ends_with_angle
        = cpp_type_str[cpp_type_str.length()-1] == '>';

      // innermost element initialization
      std::stringstream base_init;
      if (bare_type.is_int_type()) {
        base_init << "(0)";
      } else if (bare_type.is_double_type()) {
        base_init << "(DUMMY_VAR__)";
      } else if (bare_type.is_vector_type()
                 || bare_type.is_row_vector_type()) {
        base_init << "(";
        generate_expression(arg1, NOT_USER_FACING, base_init);
        base_init << ")";
      } else if (bare_type.is_matrix_type()) {
        base_init << "(";
        generate_expression(arg1, NOT_USER_FACING, base_init);
        base_init << ", ";
        generate_expression(arg2, NOT_USER_FACING, base_init);
        base_init << ")";
      } else {
        // shouldn't get here
        base_init << "()";
      }

      // for array dimensions, init for each dimension is:
      // <size dim-n>, (n-1) nested vectors of cpp_decl_type
      int ct = ar_lens.size() - 1;  // tracks nesting
      for (size_t i = 0; i < ar_lens.size(); ++i, --ct) {
        o << "(";
        generate_expression(ar_lens[i], NOT_USER_FACING, o);
        o << ", ";
        for (int j = 0; j < ct; ++j)
          o << "std::vector<";
        o << cpp_type_str;
        for (int j = 0; j < ct; ++j) {
          if (j > 0 || ends_with_angle)
            o << " ";  // maybe not needed for c++11
          o << ">";
        }
      }
      o << base_init.str();
      for (size_t i = 0; i < ar_lens.size(); ++i)
        o << ")";
    }
  }
}
#endif
