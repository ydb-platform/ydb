#ifndef STAN_LANG_GENERATOR_GENERATE_VOID_STATEMENT_HPP
#define STAN_LANG_GENERATOR_GENERATE_VOID_STATEMENT_HPP

#include <stan/lang/generator/generate_indent.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate a dummy void-cast statement for a variable of the
     * specified name at the specified indentation level to the
     * specified output stream.
     *
     * @param[in] name variable name
     * @param[in] indent indentation level
     * @param[in,out] o stream for genering
     */
    void generate_void_statement(const std::string& name,
                                 const size_t indent,
                                 std::ostream& o)  {
      generate_indent(indent, o);
      o << "(void) " << name << ";"
        << "  // dummy to suppress unused var warning"
        << EOL;
    }



  }
}
#endif
