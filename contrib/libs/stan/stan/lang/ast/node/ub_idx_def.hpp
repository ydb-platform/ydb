#ifndef STAN_LANG_AST_NODE_UB_IDX_DEF_HPP
#define STAN_LANG_AST_NODE_UB_IDX_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    ub_idx::ub_idx() { }

    ub_idx::ub_idx(const expression& ub) : ub_(ub) { }

  }
}
#endif
