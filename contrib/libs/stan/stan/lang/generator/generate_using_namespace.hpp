#ifndef STAN_LANG_GENERATOR_GENERATE_USING_NAMESPACE_HPP
#define STAN_LANG_GENERATOR_GENERATE_USING_NAMESPACE_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate using statement for the specified namespace to the
     * specfied stream.
     *
     * @param[in] ns namespace for which using statement is generated
     * @param[in,out] o stream for generating
     */
    void generate_using_namespace(const std::string& ns, std::ostream& o) {
      o << "using namespace " << ns << ";" << EOL;
    }


  }
}
#endif
