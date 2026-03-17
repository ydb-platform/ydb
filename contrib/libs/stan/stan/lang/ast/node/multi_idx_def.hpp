#ifndef STAN_LANG_AST_NODE_MULTI_IDX_DEF_HPP
#define STAN_LANG_AST_NODE_MULTI_IDX_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    multi_idx::multi_idx() { }

    multi_idx::multi_idx(const expression& idxs) : idxs_(idxs) { }

  }
}
#endif
