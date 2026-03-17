#ifndef STAN_LANG_GENERATOR_GENERATE_GLOBALS_HPP
#define STAN_LANG_GENERATOR_GENERATE_GLOBALS_HPP

#include <stan/lang/generator/constants.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the global variables to the specified stream.
     *
     * @param[in] o stream for generating
     */
    void generate_globals(std::ostream& o) {
      o << "static int current_statement_begin__;" << EOL2;
    }

  }
}
#endif
