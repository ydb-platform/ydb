#ifndef STAN_LANG_GENERATOR_WRITE_BEGIN_ALL_DIMS_ROW_MAJ_LOOP_HPP
#define STAN_LANG_GENERATOR_WRITE_BEGIN_ALL_DIMS_ROW_MAJ_LOOP_HPP

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
     * Indexing order is row major: array dims 1...N then row, col
     *
     * @param[in] var_decl variable declaration
     * @param[in] declare_size_vars if true, generate size_t var decls
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */

    void write_begin_all_dims_row_maj_loop(const block_var_decl& var_decl,
                                           bool declare_size_vars,
                                           int indent, std::ostream& o) {
      std::string name(var_decl.name());
      expression arg1(var_decl.type().innermost_type().arg1());
      expression arg2(var_decl.type().innermost_type().arg2());
      std::vector<expression> ar_var_dims = var_decl.type().array_lens();

      for (size_t i = 0; i < ar_var_dims.size(); ++i) {
        generate_indent(indent, o);
        if (declare_size_vars)
          o << "size_t ";
        o << name << "_k_" << i << "_max__ = ";
        generate_expression(ar_var_dims[i], NOT_USER_FACING, o);
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
      if (!is_nil(arg2)) {
        generate_indent(indent, o);
        if (declare_size_vars)
          o << "size_t ";
        o << name << "_j_2_max__ = ";
        generate_expression(arg2, NOT_USER_FACING, o);
        o << ";" << EOL;
      }

      // nested for stmts open
      for (size_t i = 0; i < ar_var_dims.size(); ++i) {
        generate_indent(indent++, o);
        o << "for (size_t k_"  << i << "__ = 0;"
          << " k_" << i << "__ < "
          << name << "_k_" << i  << "_max__;"
          << " ++k_" << i << "__) {" << EOL;
      }
      if (!is_nil(arg1)) {
        generate_indent(indent++, o);
        o << "for (size_t j_1__ = 0; "
          << "j_1__ < " << name << "_j_1_max__;"
          << " ++j_1__) {" << EOL;
      }
      if (!is_nil(arg2)) {
        generate_indent(indent++, o);
        o << "for (size_t j_2__ = 0; "
          << "j_2__ < " << name << "_j_2_max__;"
          << " ++j_2__) {" << EOL;
      }
    }

  }
}
#endif
