#ifndef STAN_LANG_GENERATOR_GENERATE_LOCAL_VAR_DECL_INITS_HPP
#define STAN_LANG_GENERATOR_GENERATE_LOCAL_VAR_DECL_INITS_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <stan/lang/generator/generate_initializer.hpp>
#include <stan/lang/generator/generate_validate_var_dims.hpp>
#include <stan/lang/generator/generate_void_statement.hpp>
#include <stan/lang/generator/write_var_decl_arg.hpp>
#include <stan/lang/generator/write_var_decl_type.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate local variable declarations, including
     * initializations, for the specified declarations, indentation
     * level, writing to the specified stream.
     * Generated code is preceeded by stmt updating global variable
     * `current_statement_begin__` to src file line number where
     * variable is declared.
     *
     * @param[in] vs variable declarations
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void generate_local_var_decl_inits(const std::vector<local_var_decl>& vs,
                                       int indent, std::ostream& o) {
      for (size_t i = 0; i < vs.size(); ++i) {
        generate_indent(indent, o);
        o << "current_statement_begin__ = " <<  vs[i].begin_line_ << ";"
          << EOL;

        // validate dimensions before declaration
        if (vs[i].type().num_dims() > 0)
          generate_validate_var_dims(vs[i], indent, o);

        // declare
        std::string var_name(vs[i].name());
        local_var_type ltype = vs[i].type().innermost_type();
        std::string cpp_type_str = get_verbose_var_type(ltype.bare_type());
        write_var_decl_type(ltype.bare_type(), cpp_type_str,
                            vs[i].type().array_dims(), indent, o);
        o << " " << var_name;
        write_var_decl_arg(ltype.bare_type(), cpp_type_str,
                           vs[i].type().array_lens(),
                           ltype.arg1(), ltype.arg2(), o);
        o << ";" << EOL;

        // initialize
        if (vs[i].type().num_dims() == 0)
          generate_void_statement(var_name, indent, o);
        if (!ltype.bare_type().is_int_type()) {
          generate_indent(indent, o);
          o << "stan::math::initialize(" << var_name << ", DUMMY_VAR__);"
            << EOL;
        }

        // fill
        generate_indent(indent, o);
        o << "stan::math::fill(" << var_name << ", "
          << (ltype.bare_type().is_int_type() ?
              "std::numeric_limits<int>::min()" : "DUMMY_VAR__")
          << ");" << EOL;

        // define
        if (vs[i].has_def()) {
          generate_indent(indent, o);
          o << "stan::math::assign("
            << vs[i].name()
            << ",";
          generate_expression(vs[i].def(), NOT_USER_FACING, o);
          o << ");" << EOL;
        }
        o << EOL;
      }
    }

  }
}
#endif
