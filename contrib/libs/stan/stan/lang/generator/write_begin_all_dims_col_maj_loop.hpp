#ifndef STAN_LANG_GENERATOR_WRITE_BEGIN_ALL_DIMS_COL_MAJ_LOOP_HPP
#define STAN_LANG_GENERATOR_WRITE_BEGIN_ALL_DIMS_COL_MAJ_LOOP_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_expression.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate the openings of a sequence of zero or more for loops
     * corresponding to all dimensions of a variable, with the
     * specified indentation level writing to the specified stream.
     * If specified, declare named size_t variable for each dimension
     * which avoids re-evaluation of size expression on each iteration.
     *
     * Indexing order is column major, nesting is innermost to outermost
     * e.g., 3-d array of matrices indexing order:  col, row, d3, d2, d1
     *
     * @param[in] var_decl variable declaration
     * @param[in] declare_size_vars if true, generate size_t var decls
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void write_begin_all_dims_col_maj_loop(const block_var_decl& var_decl,
                                           bool declare_size_vars,
                                           int indent, std::ostream& o) {
      std::string name(var_decl.name());
      expression arg1(var_decl.type().innermost_type().arg1());
      expression arg2(var_decl.type().innermost_type().arg2());
      std::vector<expression> ar_var_dims = var_decl.type().array_lens();

      if (!is_nil(arg2)) {
        generate_indent(indent, o);
        if (declare_size_vars)
          o << "size_t ";
        o << name << "_j_2_max__ = ";
        generate_expression(arg2, NOT_USER_FACING, o);
        o << ";" << EOL;
      }
      if (!is_nil(arg1)) {
        generate_indent(indent, o);
        if (declare_size_vars)
          o << "size_t ";
        o << name << "_j_1_max__ = ";
        generate_expression(arg1, NOT_USER_FACING, o);
        o << ";" << EOL;
      }
      for (size_t i = 0; i < ar_var_dims.size(); ++i) {
        generate_indent(indent, o);
        if (declare_size_vars)
          o << "size_t ";
        o << name << "_k_" << i << "_max__ = ";
        generate_expression(ar_var_dims[i], NOT_USER_FACING, o);
        o << ";" << EOL;
      }

      // nested for stmts open, row, col indexes
      if (!is_nil(arg2)) {
        generate_indent(indent++, o);
        o << "for (size_t j_2__ = 0; "
          << "j_2__ < " << name << "_j_2_max__;"
          << " ++j_2__) {" << EOL;
      }
      if (!is_nil(arg1)) {
        generate_indent(indent++, o);
        o << "for (size_t j_1__ = 0; "
          << "j_1__ < " << name << "_j_1_max__;"
          << " ++j_1__) {" << EOL;
      }
      for (size_t i = ar_var_dims.size(); i > 0; --i) {
        int idx = i - 1;   // size == N, indexes run from 0 .. N - 1
        generate_indent(indent++, o);
        o << "for (size_t k_"  << idx << "__ = 0;"
          << " k_" << idx << "__ < "
          << name << "_k_" << idx  << "_max__;"
          << " ++k_" << idx << "__) {" << EOL;
      }
    }

  }
}
#endif
