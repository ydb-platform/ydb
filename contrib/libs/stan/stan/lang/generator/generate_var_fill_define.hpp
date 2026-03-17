#ifndef STAN_LANG_GENERATOR_GENERATE_VAR_FILL_DEFINE_HPP
#define STAN_LANG_GENERATOR_GENERATE_VAR_FILL_DEFINE_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_expression.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <ostream>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate stmts to fill variable, followed by assignment statement
     * for definition, if any.
     *
     * @param[in] var_decl variable declarations
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void generate_var_fill_define(const block_var_decl& var_decl,
                                  int indent, std::ostream& o) {
      block_var_type btype = var_decl.type().innermost_type();

      // fill
      generate_indent(indent, o);
      if (btype.bare_type().is_int_type()) {
        o << "stan::math::fill(" << var_decl.name()
          << ", std::numeric_limits<int>::min());"
          << EOL;
      } else {
        o << "stan::math::fill(" << var_decl.name() << ", DUMMY_VAR__);" << EOL;
      }

      // define
      if (var_decl.has_def()) {
        generate_indent(indent, o);
        o << "stan::math::assign("
          << var_decl.name()
          << ",";
        generate_expression(var_decl.def(), NOT_USER_FACING, o);
        o << ");" << EOL;
      }
    }

  }
}
#endif
