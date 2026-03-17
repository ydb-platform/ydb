#ifndef STAN_LANG_GENERATOR_GENERATE_NAMESPACE_END_HPP
#define STAN_LANG_GENERATOR_GENERATE_NAMESPACE_END_HPP

#include <stan/lang/generator/constants.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the end of a namespace to the specified stream.
     *
     * @param[in, out] o stream for generating
     */
    void generate_namespace_end(std::ostream& o) {
      o << "}  // namespace" << EOL2;
    }

  }
}
#endif
