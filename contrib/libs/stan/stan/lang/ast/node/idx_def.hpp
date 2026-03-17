#ifndef STAN_LANG_AST_NODE_IDX_DEF_HPP
#define STAN_LANG_AST_NODE_IDX_DEF_HPP

#include <stan/lang/ast.hpp>
#include <boost/variant/apply_visitor.hpp>
#include <string>


namespace stan {
  namespace lang {

    idx::idx() { }

    idx::idx(const uni_idx& i) : idx_(i) { }

    idx::idx(const multi_idx& i) : idx_(i) { }

    idx::idx(const omni_idx& i) : idx_(i) { }

    idx::idx(const lb_idx& i) : idx_(i) { }

    idx::idx(const ub_idx& i) : idx_(i) { }

    idx::idx(const lub_idx& i) : idx_(i) { }

    std::string idx::to_string() const {
      write_idx_vis vis;
      return boost::apply_visitor(vis, idx_);
    }
  }
}
#endif
