#ifndef STAN_LANG_GENERATOR_GENERATE_PROPTO_DEFAULT_FUNCTION_HPP
#define STAN_LANG_GENERATOR_GENERATE_PROPTO_DEFAULT_FUNCTION_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/generate_function_arguments.hpp>
#include <stan/lang/generator/generate_function_inline_return_type.hpp>
#include <stan/lang/generator/generate_function_name.hpp>
#include <stan/lang/generator/generate_function_template_parameters.hpp>
#include <stan/lang/generator/generate_propto_default_function_body.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate a version of the specified function with propto set to
     * false, with the specified local scalar type, writing to the
     * specified stream.
     *
     * @param[in] fun function declaration
     * @param[in] scalar_t_name string representation of scalar type
     * for local scalar variables
     * @param[in,out] o stream for generating
     */
    void generate_propto_default_function(const function_decl_def& fun,
                                          const std::string& scalar_t_name,
                                          std::ostream& o) {
      generate_function_template_parameters(fun, false, false, false, o);
      generate_function_inline_return_type(fun, scalar_t_name, 0, o);
      generate_function_name(fun, o);
      generate_function_arguments(fun, false, false, false, o);
      generate_propto_default_function_body(fun, o);
    }



  }
}
#endif
