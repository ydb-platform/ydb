#ifndef STAN_LANG_GENERATOR_GENERATE_EXPRESSION_HPP
#define STAN_LANG_GENERATOR_GENERATE_EXPRESSION_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/expression_visgen.hpp>
#include <boost/variant/apply_visitor.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the specified expression to the specified stream with
     * user-facing/C++ format and parameter/data format controlled by
     * the flags.
     *
     * @param[in] e expression to generate
     * @param[in] user_facing true if expression might be reported to user
     * @param[in,out] o stream for generating
     */
    void generate_expression(const expression& e, bool user_facing,
                             std::ostream& o) {
      expression_visgen vis(o, user_facing);
      boost::apply_visitor(vis, e.expr_);
    }

  }
}
#endif
