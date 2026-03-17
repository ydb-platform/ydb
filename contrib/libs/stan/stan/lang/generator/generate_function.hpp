#ifndef STAN_LANG_GENERATOR_GENERATE_FUNCTION_HPP
#define STAN_LANG_GENERATOR_GENERATE_FUNCTION_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/fun_scalar_type.hpp>
#include <stan/lang/generator/generate_function_arguments.hpp>
#include <stan/lang/generator/generate_function_body.hpp>
#include <stan/lang/generator/generate_function_inline_return_type.hpp>
#include <stan/lang/generator/generate_function_name.hpp>
#include <stan/lang/generator/generate_function_template_parameters.hpp>
#include <stan/lang/generator/generate_propto_default_function.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate the specified function and optionally its default for
     * propto=false for functions ending in _log.
     *
     * Exact behavior differs for unmarked functions, and functions
     * ending in one of "_rng", "_lp", or "_log".
     *
     * @param[in] fun function AST object
     * @param[in, out] out output stream to which function definition
     * is written
     */
    void generate_function(const function_decl_def& fun,
                           std::ostream& out) {
      bool is_rng = ends_with("_rng", fun.name_);
      bool is_lp = ends_with("_lp", fun.name_);
      bool is_pf = ends_with("_log", fun.name_)
        || ends_with("_lpdf", fun.name_) || ends_with("_lpmf", fun.name_);
      std::string scalar_t_name = fun_scalar_type(fun, is_lp);

      generate_function_template_parameters(fun, is_rng, is_lp, is_pf, out);
      generate_function_inline_return_type(fun, scalar_t_name, 0, out);
      generate_function_name(fun, out);

      generate_function_arguments(fun, is_rng, is_lp, is_pf, out);
      generate_function_body(fun, scalar_t_name, out);

      // need a second function def for default propto=false for _log
      // funs; but don't want duplicate def, so don't do it for
      // forward decl when body is no-op
      if (is_pf && !fun.body_.is_no_op_statement())
        generate_propto_default_function(fun, scalar_t_name, out);
      out << EOL;
    }

  }
}
#endif
