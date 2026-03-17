#ifndef STAN_LANG_GENERATOR_GENERATE_INCLUDE_HPP
#define STAN_LANG_GENERATOR_GENERATE_INCLUDE_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate an include statement for the specified library path.
     *
     * @param lib_name path to library
     * @param o stream for generating
     */
    void generate_include(const std::string& lib_name, std::ostream& o) {
      o << "#include" << " " << "<" << lib_name << ">" << EOL;
    }



  }
}
#endif
