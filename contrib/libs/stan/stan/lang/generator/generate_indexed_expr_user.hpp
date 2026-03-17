#ifndef STAN_LANG_GENERATOR_GENERATE_INDEXED_EXPR_USER_HPP
#define STAN_LANG_GENERATOR_GENERATE_INDEXED_EXPR_USER_HPP

#include <stan/lang/ast.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    void generate_expression(const expression& e, bool user_facing,
                             std::ostream& o);

    /**
     * Generate an expression with indices, writing brackets around
     * indices and commas in between as necessary.  If no indices are
     * presents, no brackets will be written.
     *
     * @param[in] expr expression for indexing
     * @param[in] indexes sequence of indexes
     * @param[in,out] o stream for writing
     */
    void generate_indexed_expr_user(const std::string& expr,
                                    const std::vector<expression> indexes,
                                    std::ostream& o) {
      static const bool user_facing = true;
      o << expr;
      if (indexes.size() == 0) return;
      o << '[';
      for (size_t i = 0; i < indexes.size(); ++i) {
        if (i > 0) o << ", ";
        generate_expression(indexes[i], user_facing, o);
      }
      o << ']';
    }

  }
}
#endif
