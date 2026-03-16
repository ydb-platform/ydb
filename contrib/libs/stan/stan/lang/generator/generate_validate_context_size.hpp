#ifndef STAN_LANG_GENERATOR_GENERATE_VALIDATE_CONTEXT_SIZE_HPP
#define STAN_LANG_GENERATOR_GENERATE_VALIDATE_CONTEXT_SIZE_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_expression.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <stan/lang/generator/generate_validate_nonnegative.hpp>
#include <stan/lang/generator/get_typedef_var_type.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /*
     * Generates code to validate data variables to make sure they
     * only use positive dimension sizes and that the var_context
     * out of which they are read have matching dimension sizes.
     *
     * @param[in] var_decl block variable declaration
     * @param[in] stage id string for error msgs
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void generate_validate_context_size(const block_var_decl& var_decl,
                                        const std::string& stage,
                                        size_t indent,
                                        std::ostream& o) {
      std::string var_name(var_decl.name());
      block_var_type btype = var_decl.type().innermost_type();

      std::vector<expression> array_dim_sizes = var_decl.type().array_lens();
      expression arg1 = btype.arg1();
      expression arg2 = btype.arg2();

      // check declared sizes against actual sizes
      generate_indent(indent, o);
      o << "context__.validate_dims("
        << '"' << stage << '"' << ", "
        << '"' << var_name << '"' << ", "
        << '"' << get_typedef_var_type(btype.bare_type()) << '"' << ", "
        << "context__.to_vec(";
      for (size_t i = 0; i < array_dim_sizes.size(); ++i) {
        if (i > 0) o << ",";
        generate_expression(array_dim_sizes[i].expr_, NOT_USER_FACING, o);
      }
      if (!is_nil(arg1)) {
        if (array_dim_sizes.size() > 0) o << ",";
        generate_expression(arg1.expr_, NOT_USER_FACING, o);
        if (!is_nil(arg2)) {
          o << ",";
          generate_expression(arg2.expr_, NOT_USER_FACING, o);
        }
      }
      o << "));" << EOL;
    }

  }
}
#endif
