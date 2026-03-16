#ifndef STAN_LANG_GENERATOR_GENERATE_PRIVATE_DECL_HPP
#define STAN_LANG_GENERATOR_GENERATE_PRIVATE_DECL_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the private declaration scope for a class to the
     * specified stream.
     *
     * @param[in,out] o stream for generating
     */
    void generate_private_decl(std::ostream& o) {
      o << "private:" << EOL;
    }

  }
}
#endif
