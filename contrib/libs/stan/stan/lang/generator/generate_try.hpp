#ifndef STAN_LANG_GENERATOR_GENERATE_TRY_HPP
#define STAN_LANG_GENERATOR_GENERATE_TRY_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the opening for a try statement at the specified
     * indentation level on the specified stream.
     *
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void generate_try(int indent, std::ostream& o) {
      generate_indent(indent, o);
      o << "try {" << EOL;
    }

  }
}
#endif
