#ifndef STAN_LANG_GENERATOR_GENERATE_QUOTED_STRING_HPP
#define STAN_LANG_GENERATOR_GENERATE_QUOTED_STRING_HPP

#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Print the specified string to the specified output stream,
     * wrapping in double quotes (") and inserting a backslash to
     * escape double quotes, single quotes, and backslashes.
     *
     * @param[in] s String to output
     * @param[in,out] o Output stream
     */
    void generate_quoted_string(const std::string& s, std::ostream& o) {
      o << '"';
      for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '"' || s[i] == '\\' || s[i] == '\'' )
          o << '\\';
        o << s[i];
      }
      o << '"';
    }



  }
}
#endif
