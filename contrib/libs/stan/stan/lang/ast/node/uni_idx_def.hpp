#ifndef STAN_LANG_AST_NODE_UNI_IDX_DEF_HPP
#define STAN_LANG_AST_NODE_UNI_IDX_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    uni_idx::uni_idx() { }

    uni_idx::uni_idx(const expression& idx) : idx_(idx) { }

  }
}
#endif
