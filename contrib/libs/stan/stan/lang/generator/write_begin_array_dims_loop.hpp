#ifndef STAN_LANG_GENERATOR_WRITE_BEGIN_ARRAY_DIMS_LOOP_HPP
#define STAN_LANG_GENERATOR_WRITE_BEGIN_ARRAY_DIMS_LOOP_HPP

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
     * corresponding to array dimensions of a variable, with the
     * specified indentation level writing to the specified stream.
     * If specified, declare named size_t variable for each dimension
     * which avoids re-evaluation of size expression on each iteration.
     *
     * @param[in] var_decl variable declaration
     * @param[in] declare_size_vars if true, generate size_t var decls
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void write_begin_array_dims_loop(const block_var_decl& var_decl,
                                     bool declare_size_vars,
                                     int indent, std::ostream& o) {
      std::string name(var_decl.name());
      std::vector<expression> ar_var_dims = var_decl.type().array_lens();

      for (size_t i = 0; i < ar_var_dims.size(); ++i) {
        generate_indent(indent, o);
        if (declare_size_vars)
          o << "size_t ";
        o << name << "_i_" << i << "_max__ = ";
        generate_expression(ar_var_dims[i], NOT_USER_FACING, o);
        o << ";" << EOL;
      }
      for (size_t i = 0; i < ar_var_dims.size(); ++i) {
        generate_indent(indent + i, o);
        o << "for (size_t i_"  << i << "__ = 0;"
          << " i_" << i << "__ < " << name << "_i_" << i << "_max__;"
          << " ++i_" << i << "__) {" << EOL;
      }
    }

  }
}
#endif
