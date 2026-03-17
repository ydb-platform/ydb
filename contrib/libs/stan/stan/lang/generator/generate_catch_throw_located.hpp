#ifndef STAN_LANG_GENERATOR_GENERATE_CATCH_THROW_LOCATED_HPP
#define STAN_LANG_GENERATOR_GENERATE_CATCH_THROW_LOCATED_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_comment.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate code to catch and rethrow an exception that includes
     * location information read out of the generated context.
     *
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating.
     */
    void generate_catch_throw_located(int indent, std::ostream& o) {
      generate_indent(indent, o);
      o << "} catch (const std::exception& e) {" << EOL;
      generate_indent(indent + 1, o);
      o << "stan::lang::rethrow_located(e, current_statement_begin__"
        << ", prog_reader__());"
        << EOL;
      generate_comment("Next line prevents compiler griping about no return",
                       indent + 1, o);
      generate_indent(indent + 1, o);
      o << "throw std::runtime_error"
        << "(\"*** IF YOU SEE THIS, PLEASE REPORT A BUG ***\");" << EOL;
      generate_indent(indent, o);
      o << "}" << EOL;
    }



  }
}
#endif
