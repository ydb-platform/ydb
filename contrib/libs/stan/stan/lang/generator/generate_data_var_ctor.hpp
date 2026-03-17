#ifndef STAN_LANG_GENERATOR_GENERATE_DATA_VAR_CTOR_HPP
#define STAN_LANG_GENERATOR_GENERATE_DATA_VAR_CTOR_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <stan/lang/generator/generate_var_constructor.hpp>
#include <string>
#include <ostream>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate code to to the specified stream to instantiate
     * a member variable declared in data block by calling the appropriate constructor.
     * Doesn't check variable dimensions - should have already been done 
     * as part of checks on var_context.
     *
     * @param[in] var_decl block variable declaration
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void generate_data_var_ctor(const block_var_decl& var_decl,
                                int indent, std::ostream& o) {
      std::string var_name(var_decl.name());
      block_var_type btype = var_decl.type().innermost_type();
      generate_indent(indent, o);
      o << var_name << " = ";
      if (var_decl.bare_type().is_int_type()) {
        o << "int(0)";
      } else if (var_decl.bare_type().is_double_type()) {
        o << "double(0)";
      } else {
        generate_var_constructor(var_decl, "double", o);
      }
      o << ";" << EOL;
    }
  }
}
#endif
