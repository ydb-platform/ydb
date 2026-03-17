#ifndef STAN_LANG_AST_NODE_ASSGN_DEF_HPP
#define STAN_LANG_AST_NODE_ASSGN_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    assgn::assgn() { }

    assgn::assgn(const variable& lhs_var, const std::vector<idx>& idxs,
                 const std::string& op, const expression& rhs)
      : lhs_var_(lhs_var), idxs_(idxs), op_(op), rhs_(rhs) { }

    bool assgn::is_simple_assignment() const {
      return !op_.compare("=");
    }

    bool assgn::lhs_var_has_sliced_idx() const {
      for (const auto& idx : idxs_)
        if (is_multi_index(idx)) return true;
      return false;
    }

    bool assgn::lhs_var_occurs_on_rhs() const {
      var_occurs_vis vis(lhs_var_);
      return boost::apply_visitor(vis, rhs_.expr_);
    }

  }
}
#endif
