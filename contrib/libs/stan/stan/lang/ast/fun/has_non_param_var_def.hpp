#ifndef STAN_LANG_AST_FUN_HAS_NON_PARAM_VAR_DEF_HPP
#define STAN_LANG_AST_FUN_HAS_NON_PARAM_VAR_DEF_HPP

#include <stan/lang/ast.hpp>
#include <boost/variant/apply_visitor.hpp>

namespace stan {
  namespace lang {

    bool has_non_param_var(const expression& e,
                           const variable_map& var_map) {
      has_non_param_var_vis vis(var_map);
      return boost::apply_visitor(vis, e.expr_);
    }

  }
}
#endif
