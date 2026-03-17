#ifndef STAN_LANG_GENERATOR_GENERATE_USING_HPP
#define STAN_LANG_GENERATOR_GENERATE_USING_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate a using statement for the specified type to the
     * specified stream.
     *
     * @param[in] type type for which using statement is geneated
     * @param[in,out] o stream for generation
     */
    void generate_using(const std::string& type, std::ostream& o) {
      o << "using " << type << ";" << EOL;
    }

  }
}
#endif
