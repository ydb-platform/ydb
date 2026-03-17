#ifndef STAN_LANG_GENERATOR_GENERATE_FUNCTION_BODY_HPP
#define STAN_LANG_GENERATOR_GENERATE_FUNCTION_BODY_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/generate_catch_throw_located.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_statement.hpp>
#include <stan/lang/generator/generate_try.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate the body of the specified function, with the specified
     * local scalar type, writing to the specified stream.
     *
     * @param[in] fun function declaration
     * @param[in] scalar_t_name name of type to use for scalars in the
     * function body
     * @param[in,out] o stream for generating
     */
    void generate_function_body(const function_decl_def& fun,
                                const std::string& scalar_t_name,
                                std::ostream& o) {
      if (fun.body_.is_no_op_statement()) {
        o << ";" << EOL;
        return;
      }
      o << " {" << EOL;
      o << INDENT << "typedef " << scalar_t_name << " local_scalar_t__;" << EOL;
      o << INDENT << "typedef "
        << (fun.return_type_.innermost_type().is_int_type()
            ? "int" : "local_scalar_t__")
        << " fun_return_scalar_t__;" << EOL;
      o << INDENT
        << "const static bool propto__ = true;" << EOL
        << INDENT << "(void) propto__;" << EOL;
      // use this dummy for inits
      o << INDENT2 << "local_scalar_t__ "
        << "DUMMY_VAR__(std::numeric_limits<double>::quiet_NaN());" << EOL;
      o << INDENT2 << "(void) DUMMY_VAR__;  // suppress unused var warning"
        << EOL2;

      o << INDENT << "int current_statement_begin__ = -1;" << EOL;
      generate_try(1, o);
      generate_statement(fun.body_, 2, o);
      generate_catch_throw_located(1, o);
      o << "}" << EOL;
    }

  }
}
#endif
