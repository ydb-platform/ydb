#ifndef STAN_LANG_GENERATOR_GENERATE_USINGS_HPP
#define STAN_LANG_GENERATOR_GENERATE_USINGS_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_using.hpp>
#include <stan/lang/generator/generate_using_namespace.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the using statements for a Stan model.
     *
     * @param[in,out] o stream for generating
     */
    void generate_usings(std::ostream& o) {
      generate_using("std::istream", o);
      generate_using("std::string", o);
      generate_using("std::stringstream", o);
      generate_using("std::vector", o);
      generate_using("stan::io::dump", o);
      generate_using("stan::math::lgamma", o);
      generate_using("stan::model::prob_grad", o);
      generate_using_namespace("stan::math", o);
      o << EOL;
    }

  }
}
#endif
