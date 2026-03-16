#ifndef STAN_LANG_GENERATOR_GENERATE_STATEMENTS_HPP
#define STAN_LANG_GENERATOR_GENERATE_STATEMENTS_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/generate_statement.hpp>
#include <ostream>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate the set of statements in a program block with
     * the specified indentation level on the specified stream.
     *
     * @param[in] statements vector of statements
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void generate_statements(const std::vector<statement> statements,
                             int indent, std::ostream& o) {
      for (size_t i = 0; i < statements.size(); ++i)
        generate_statement(statements[i], indent, o);
    }

  }
}
#endif

