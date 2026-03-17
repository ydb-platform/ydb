#ifndef STAN_LANG_GENERATOR_GENERATE_LINE_NUMBER_HPP
#define STAN_LANG_GENERATOR_GENERATE_LINE_NUMBER_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <ostream>
#include <vector>

namespace stan {
  namespace lang {

    /*
     * Generate statement to update current line number in program.
     *
     * @param[in] line program node
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    template <typename T>
    void generate_line_number(const T& line,
                              int indent, std::ostream& o) {
      o << "current_statement_begin__ = " <<  line.begin_line_ << ";" << EOL;
    }

    template void generate_line_number(const block_var_decl&,
                                       int indent, std::ostream& o);

    template void generate_line_number(const local_var_decl&,
                                       int indent, std::ostream& o);

    template void generate_line_number(const statement&,
                                       int indent, std::ostream& o);
  }
}
#endif
