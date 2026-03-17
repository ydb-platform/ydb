#ifndef STAN_LANG_GENERATOR_GENERATE_FUNCTION_FUNCTOR_HPP
#define STAN_LANG_GENERATOR_GENERATE_FUNCTION_FUNCTOR_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/fun_scalar_type.hpp>
#include <stan/lang/generator/generate_function_arguments.hpp>
#include <stan/lang/generator/generate_function_inline_return_type.hpp>
#include <stan/lang/generator/generate_function_name.hpp>
#include <stan/lang/generator/generate_function_template_parameters.hpp>
#include <stan/lang/generator/generate_functor_arguments.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate the functor to accompnay a function with the specified
     * declaration, writing to the specified stream.
     *
     * @param[in] fun function declaration
     * @param[in,out] o stream for generating
     */
    void generate_function_functor(const function_decl_def& fun,
                                   std::ostream& o) {
      if (fun.body_.is_no_op_statement())
        return;   // forward declaration, so no functor needed

      bool is_rng = ends_with("_rng", fun.name_);
      bool is_lp = ends_with("_lp", fun.name_);
      bool is_pf = ends_with("_log", fun.name_)
        || ends_with("_lpdf", fun.name_) || ends_with("_lpmf", fun.name_);
      std::string scalar_t_name = fun_scalar_type(fun, is_lp);

      o << EOL << "struct ";
      generate_function_name(fun, o);
      o << "_functor__ {" << EOL;

      o << INDENT;
      generate_function_template_parameters(fun, is_rng, is_lp, is_pf, o);

      o << INDENT;
      generate_function_inline_return_type(fun, scalar_t_name, 1, o);

      o <<  INDENT << "operator()";
      generate_function_arguments(fun, is_rng, is_lp, is_pf, o);
      o << " const {" << EOL;

      o << INDENT2 << "return ";
      generate_function_name(fun, o);
      generate_functor_arguments(fun, is_rng, is_lp, is_pf, o);
      o << ";" << EOL;
      o << INDENT << "}"  << EOL;
      o << "};" << EOL2;
    }

  }
}
#endif
