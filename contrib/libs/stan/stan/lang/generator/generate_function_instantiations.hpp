#ifndef STAN_LANG_GENERATOR_GENERATE_FUNCTION_INSTANTIATONS_HPP
#define STAN_LANG_GENERATOR_GENERATE_FUNCTION_INSTANTIATONS_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/generate_function_instantiation.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate instantiations of templated functions with non-variable 
     * parametersfor standalone generation of functions.
     *
     * @param[in] funs sequence of function declarations and
     *   definitions
     * @param[in] namespaces vector of strings used to generate the 
     *   namespaces generated code is nested in.
     * @param[in,out] o stream for generating
     */
    void generate_function_instantiations(
           const std::vector<function_decl_def>& funs,
           const std::vector<std::string>& namespaces,
           std::ostream& o) {
      for (size_t i = 0; i < funs.size(); ++i) {
        generate_function_instantiation(funs[i], namespaces, o);
      }
    }

  }
}
#endif
