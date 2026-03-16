#ifndef STAN_LANG_AST_NODE_LB_IDX_DEF_HPP
#define STAN_LANG_AST_NODE_LB_IDX_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    lb_idx::lb_idx() { }

    lb_idx::lb_idx(const expression& lb) : lb_(lb) { }

  }
}
#endif
