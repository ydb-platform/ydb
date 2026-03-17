#ifndef STAN_LANG_GENERATOR_WRITE_END_LOOP_HPP
#define STAN_LANG_GENERATOR_WRITE_END_LOOP_HPP

#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the close `}` for a sequence of zero or more nested for loops
     * with the specified indentation level writing to the specified stream.
     *
     * @param[in] dims_size dimension sizes
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void write_end_loop(size_t dims_size,
                                   int indent, std::ostream& o) {
      for (size_t i = dims_size; i > 0; --i) {
        generate_indent(indent + i - 1, o);
        o << "}" << EOL;
      }
    }

  }
}
#endif
