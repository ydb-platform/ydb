#ifndef STAN_LANG_GENERATOR_GENERATE_DATA_VAR_INIT_HPP
#define STAN_LANG_GENERATOR_GENERATE_DATA_VAR_INIT_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_expression.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <stan/lang/generator/write_begin_all_dims_col_maj_loop.hpp>
#include <stan/lang/generator/write_end_loop.hpp>
#include <stan/lang/generator/write_var_idx_all_dims.hpp>
#include <iostream>
#include <ostream>
#include <vector>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate initializations for data block variables by reading
     * dump format data from constructor variable context.
     * In dump format data, arrays are indexed in last-index major fashion,
     * which corresponds to column-major order for matrices
     * represented as two-dimensional arrays.  As a result, the first
     * indices change fastest.  Therefore loops must be constructed:
     * (col) (row) (array-dim-N) ... (array-dim-1)
     *
     * @param[in] var_decl block variable declaration
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void generate_data_var_init(const block_var_decl& var_decl,
                                int indent, std::ostream& o) {
      // setup - name, type, and var shape
      std::string var_name(var_decl.name());
      block_var_type vtype = var_decl.type();
      block_var_type el_type = var_decl.type().innermost_type();

      std::string vals("vals_r");
      if (vtype.bare_type().innermost_type().is_int_type())
        vals = "vals_i";

      generate_indent(indent, o);
      o << vals << "__ = context__." << vals
        << "(\"" << var_name << "\");" << EOL;
      generate_indent(indent, o);
      o << "pos__ = 0;" << EOL;

      write_begin_all_dims_col_maj_loop(var_decl, true, indent, o);

      // innermost loop stmt: update pos__
      generate_indent(indent + vtype.num_dims(), o);
      o << var_name;
      write_var_idx_all_dims(vtype.array_dims(),
                             vtype.num_dims() - vtype.array_dims(), o);
      o << " = " << vals << "__[pos__++];" << EOL;

      write_end_loop(vtype.num_dims(), indent, o);
    }

  }
}
#endif
