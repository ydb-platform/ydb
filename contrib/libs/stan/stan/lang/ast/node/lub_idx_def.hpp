#ifndef STAN_LANG_AST_NODE_LUB_IDX_DEF_HPP
#define STAN_LANG_AST_NODE_LUB_IDX_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    lub_idx::lub_idx() { }

    lub_idx::lub_idx(const expression& lb, const expression& ub)
      : lb_(lb), ub_(ub) {  }

  }
}
#endif
