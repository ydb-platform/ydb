#ifndef STAN_LANG_GENERATOR_GENERATE_USINGS_STANDALONE_FUNCTIONS_HPP
#define STAN_LANG_GENERATOR_GENERATE_USINGS_STANDALONE_FUNCTIONS_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_using.hpp>
#include <stan/lang/generator/generate_using_namespace.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the using statements for a Stan standalone functions file.
     *
     * @param[in,out] o stream for generating
     */
    void generate_usings_standalone_functions(std::ostream& o) {
      generate_using("std::istream", o);
      generate_using("std::string", o);
      generate_using("std::stringstream", o);
      generate_using("std::vector", o);
      generate_using_namespace("stan::math", o);
      o << EOL;
    }

  }
}
#endif
