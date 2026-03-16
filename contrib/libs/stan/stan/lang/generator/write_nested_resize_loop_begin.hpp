#ifndef STAN_LANG_GENERATOR_WRITE_NESTED_RESIZE_LOOP_BEGIN_HPP
#define STAN_LANG_GENERATOR_WRITE_NESTED_RESIZE_LOOP_BEGIN_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_expression.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <stan/lang/generator/generate_void_statement.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate the openings of a sequence of zero or more nested for loops
     * corresponding to the specified dimension sizes with the
     * specified indentation level writing to the specified stream.
     * Declare named size_t variable for each dimension size in order to avoid
     * re-evaluation of dimension size expression on each iteration.
     *
     * Dynamic initialization of parameter variables.
     *
     * @param[in] name var name
     * @param[in] dims dimension sizes
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void write_nested_resize_loop_begin(const std::string& name,
                                        const std::vector<expression>& dims,
                                        int indent, std::ostream& o) {
      if (dims.size() == 0) {
        generate_void_statement(name, indent, o);
        return;
      }

      // declare size_t var d_<n>_max__
      for (size_t i = 0; i < dims.size(); ++i) {
        generate_indent(indent, o);
        o << "size_t " << name << "_d_" << i << "_max__ = ";
        generate_expression(dims[i], NOT_USER_FACING, o);
        o << ";" << EOL;
      }

      for (size_t i = 0; i < dims.size(); ++i) {
        if (i < dims.size() - 1) {
          // dynamic allocation stmt
          generate_indent(indent + i, o);
          o << name;
          for (size_t j = 0; j < i; ++j)
            o << "[d_" << j << "__]";
          o << ".resize(" << name << "_d_" << i << "_max__);" << EOL;
        } else {
          // innermost dimension, reserve
          generate_indent(indent + i, o);
          o << name;
          for (size_t j = 0; j < i; ++j)
            o << "[d_" << j << "__]";
          o << ".reserve(" << name << "_d_" <<  i << "_max__);" << EOL;
        }

        // open for loop
        generate_indent(indent + i, o);
        o << "for (size_t d_"  << i << "__ = 0;"
          << " d_" << i << "__ < " << name << "_d_" << i << "_max__;"
          << " ++d_" << i << "__) {" << EOL;
      }
    }

  }
}
#endif
