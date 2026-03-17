#ifndef STAN_LANG_GENERATOR_GENERATE_VALIDATE_BLOCK_VAR_HPP
#define STAN_LANG_GENERATOR_GENERATE_VALIDATE_BLOCK_VAR_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <stan/lang/generator/generate_validate_var_decl.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate validation statements for bounded or specialized block variables.
     *
     * @param[in] var_decl block variable
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void generate_validate_block_var(const block_var_decl& var_decl,
                                     int indent, std::ostream& o) {
      block_var_type vtype = var_decl.type().innermost_type();
      if (vtype.is_constrained()) {
        generate_validate_var_decl(var_decl, indent, o);
        o << EOL;
      }
    }

  }
}
#endif
