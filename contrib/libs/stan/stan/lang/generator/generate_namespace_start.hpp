#ifndef STAN_LANG_GENERATOR_GENERATE_NAMESPACE_START_HPP
#define STAN_LANG_GENERATOR_GENERATE_NAMESPACE_START_HPP

#include <stan/lang/generator/constants.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate the opening name and brace for a namespace, with two
     * end of lines.
     *
     * @param[in] name name of namespace
     * @param[in,out] o stream for generating
     */
    void generate_namespace_start(const std::string& name, std::ostream& o) {
      o << "namespace " << name << "_namespace {" << EOL2;
    }

  }
}
#endif
