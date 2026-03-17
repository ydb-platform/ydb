#ifndef STAN_LANG_GENERATOR_GENERATE_INDENT_HPP
#define STAN_LANG_GENERATOR_GENERATE_INDENT_HPP

#include <stan/lang/generator/constants.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Write the specified number of indentations to the specified
     * output stream.
     *
     * @param indent number of indentations
     * @param[in, out] o stream to which indentations are written
     */
    void generate_indent(size_t indent, std::ostream& o) {
      for (size_t k = 0; k < indent; ++k)
        o << INDENT;
    }

  }
}
#endif
