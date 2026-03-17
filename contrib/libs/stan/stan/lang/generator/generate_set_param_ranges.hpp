#ifndef STAN_LANG_GENERATOR_GENERATE_SET_PARAM_RANGES_HPP
#define STAN_LANG_GENERATOR_GENERATE_SET_PARAM_RANGES_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_expression.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <stan/lang/generator/generate_validate_nonnegative.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /*
     * Generate statements in constructor body which cumulatively
     * determine the size required for the vector of param ranges and
     * the range for each parameter in the model by iterating over the
     * list of parameter variable declarations.
     * Generated code is preceeded by stmt updating global variable
     * `current_statement_begin__` to src file line number where
     * parameter variable is declared.
     *
     * @param[in] var_decls sequence of variable declarations
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void generate_set_param_ranges(const std::vector<block_var_decl>& var_decls,
                                   int indent, std::ostream& o) {
      generate_indent(indent, o);
      o << "num_params_r__ = 0U;" << EOL;
      generate_indent(indent, o);
      o << "param_ranges_i__.clear();" << EOL;

      for (size_t i = 0; i < var_decls.size(); ++i) {
        generate_indent(indent, o);
        o << "current_statement_begin__ = " <<  var_decls[i].begin_line_ << ";"
          << EOL;

        std::string var_name(var_decls[i].name());
        block_var_type eltype = var_decls[i].type().innermost_type();
        if (!is_nil(eltype.arg1()))
          generate_validate_nonnegative(var_name, eltype.arg1(), indent, o);
        if (!is_nil(eltype.arg2()))
          generate_validate_nonnegative(var_name, eltype.arg2(), indent, o);
        std::vector<expression> ar_lens(var_decls[i].type().array_lens());
        for (size_t i = 0; i < ar_lens.size(); ++i)
          generate_validate_nonnegative(var_name, ar_lens[i], indent, o);

        generate_indent(indent, o);
        o << "num_params_r__ += ";
        generate_expression(var_decls[i].type().params_total(),
                            NOT_USER_FACING, o);
        o << ";" << EOL;
      }
    }



  }
}
#endif
