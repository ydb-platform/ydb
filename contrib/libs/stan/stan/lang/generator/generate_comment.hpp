#ifndef STAN_LANG_GENERATOR_GENERATE_COMMENT_HPP
#define STAN_LANG_GENERATOR_GENERATE_COMMENT_HPP

#include <stan/lang/generator/generate_indent.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate the specified message as a comment with the specified
     * indentation and an end-of-line.
     *
     * @param[in] msg text of comment
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void generate_comment(const std::string& msg, int indent,
                          std::ostream& o) {
      generate_indent(indent, o);
      o << "// " << msg        << EOL;
    }

  }
}
#endif
