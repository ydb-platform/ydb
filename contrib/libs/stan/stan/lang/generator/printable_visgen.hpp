#ifndef STAN_LANG_GENERATOR_PRINTABLE_VISGEN_HPP
#define STAN_LANG_GENERATOR_PRINTABLE_VISGEN_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_expression.hpp>
#include <stan/lang/generator/generate_quoted_string.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * A visitor for generating strings and expressions, printing them
     * for C++.
     */
    struct printable_visgen : public visgen {
      /**
       * Construct a printable visitor that generates to the specified
       * stream.
       *
       * @param o stream for generating
       */
      explicit printable_visgen(std::ostream& o) : visgen(o) {  }

      /**
       * Generate a quoted version of the specified string, escaping
       * control characters as necessary.
       *
       * @param s string to generate
       */
      void operator()(const std::string& s) const {
        generate_quoted_string(s, o_);
      }

      /**
       * Generate the specified expression.
       *
       * @param e expression to generate
       */
      void operator()(const expression& e) const {
        generate_expression(e, NOT_USER_FACING, o_);
      }
    };

  }
}
#endif
