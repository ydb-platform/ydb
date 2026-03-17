#ifndef STAN_LANG_GENERATOR_GENERATE_INDEXED_EXPR_HPP
#define STAN_LANG_GENERATOR_GENERATE_INDEXED_EXPR_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/generate_indexed_expr_user.hpp>
#include <stan/lang/generator/generate_quoted_string.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate the specified expression indexed with the specified
     * indices with the specified base type of expression being
     * indexed, number of dimensions, and a flag indicating whether
     * the generation is for user output or C++ compilation.
     * Depending on the base type, two layers of parens may be written
     * in the underlying code.
     *
     * @tparam isLHS true if indexed expression appears on left-hand
     * side of an assignment
     * @param[in] expr string for expression
     * @param[in] indexes indexes for expression
     * @param[in] base_type base type of expression
     * @param[in] user_facing true if expression might be reported to user
     * @param[in,out] o stream for generating
     */
    template <bool isLHS>
    void generate_indexed_expr(const std::string& expr,
                               const std::vector<expression>& indexes,
                               bare_expr_type base_type,
                               bool user_facing, std::ostream& o) {
      if (user_facing) {
        generate_indexed_expr_user(expr, indexes, o);
        return;
      }
      if (indexes.size() == 0) {
        o << expr;
        return;
      }
      if (base_type.innermost_type().is_matrix_type()
          && base_type.num_dims() == indexes.size()) {
        for (size_t n = 0; n < indexes.size() - 1; ++n)
          o << (isLHS ? "get_base1_lhs(" : "get_base1(");
        o << expr;
        for (size_t n = 0; n < indexes.size() - 2 ; ++n) {
          o << ", ";
          generate_expression(indexes[n], user_facing, o);
          o << ", ";
          generate_quoted_string(expr, o);
          o << ", " << (n + 1) << ')';
        }
        o << ", ";
        generate_expression(indexes[indexes.size() - 2U], user_facing, o);
        o << ", ";
        generate_expression(indexes[indexes.size() - 1U], user_facing, o);
        o << ", ";
        generate_quoted_string(expr, o);
        o << ", " << (indexes.size() - 1U) << ')';
        return;
      }
      for (size_t n = 0; n < indexes.size(); ++n)
        o << (isLHS ? "get_base1_lhs(" : "get_base1(");
      o << expr;
      for (size_t n = 0; n < indexes.size() - 1; ++n) {
        o << ", ";
        generate_expression(indexes[n], user_facing, o);
        o << ", ";
        generate_quoted_string(expr, o);
        o << ", " << (n + 1) << ')';
      }
      o << ", ";
      generate_expression(indexes[indexes.size() - 1U], user_facing, o);
      o << ", ";
      generate_quoted_string(expr, o);
      o << ", " << (indexes.size()) << ')';
    }

  }
}
#endif
