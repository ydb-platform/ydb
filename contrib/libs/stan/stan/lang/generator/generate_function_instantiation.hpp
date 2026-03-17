#ifndef STAN_LANG_GENERATOR_GENERATE_FUNCTION_INSTANTIATION_HPP
#define STAN_LANG_GENERATOR_GENERATE_FUNCTION_INSTANTIATION_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/generate_function_inline_return_type.hpp>
#include <stan/lang/generator/generate_function_instantiation_body.hpp>
#include <stan/lang/generator/generate_function_instantiation_name.hpp>
#include <stan/lang/generator/generate_function_arguments.hpp>
#include <ostream>
#include <vector>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate a non-variable (double only) instantiation of  specified 
     * function and optionally its default for propto=false 
     * for functions ending in _log.
     *
     * Exact behavior differs for unmarked functions, and functions
     * ending in one of "_rng", "_lp", or "_log".
     *
     * @param[in] fun function AST object
     * @param[in] namespaces vector of strings used to generate the 
     *   namespaces generated code is nested in.
     * @param[in, out] out output stream to which function definition
     * is written
     */
    void generate_function_instantiation(const function_decl_def& fun,
                           const std::vector<std::string>& namespaces,
                           std::ostream& out) {
      // Do not generate anything for forward decalrations
      if (fun.body_.is_no_op_statement()) {
        return;
      }

      bool is_rng = ends_with("_rng", fun.name_);
      bool is_lp = ends_with("_lp", fun.name_);
      bool is_pf = ends_with("_log", fun.name_)
        || ends_with("_lpdf", fun.name_) || ends_with("_lpmf", fun.name_);

      // scalar type is always double for instantiations
      std::string scalar_t_name = "double";
      std::string rng_class = "boost::ecuyer1988";

      out << "// [[stan::function]]" << EOL;
      generate_function_inline_return_type(fun, scalar_t_name, 0, out);
      generate_function_instantiation_name(fun, out);
      generate_function_arguments(
        fun, is_rng, is_lp, is_pf, out, true /*no templates*/, rng_class,
        true /*parameter_defaults*/);

      generate_function_instantiation_body(
        fun, namespaces, is_rng, is_lp, is_pf, rng_class, out);

      out << EOL;
    }

  }
}
#endif
