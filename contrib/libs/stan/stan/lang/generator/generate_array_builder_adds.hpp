#ifndef STAN_LANG_GENERATOR_GENERATE_ARRAY_BUILDER_ADDS_HPP
#define STAN_LANG_GENERATOR_GENERATE_ARRAY_BUILDER_ADDS_HPP

#include <stan/lang/ast.hpp>
#include <ostream>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Recursive helper function for array, matrix, and row_vector expressions
     * which generates chain of calls to math lib array_builder add function
     * for each of the contained elements.
     *
     * @param[in] elements vector of expression elements to generate
     * @param[in] user_facing true if expression might be reported to user
     * @param[in,out] o stream for generating
     */
    void generate_array_builder_adds(const std::vector<expression>& elements,
                                     bool user_facing,
                                     std::ostream& o) {
      for (size_t i = 0; i < elements.size(); ++i) {
        o << ".add(";
        generate_expression(elements[i], user_facing, o);
        o << ")";
      }
    }

  }
}
#endif
