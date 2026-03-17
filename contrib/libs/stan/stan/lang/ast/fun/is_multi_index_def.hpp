#ifndef STAN_LANG_AST_FUN_IS_MULTI_INDEX_DEF_HPP
#define STAN_LANG_AST_FUN_IS_MULTI_INDEX_DEF_HPP

#include <stan/lang/ast.hpp>
#include <boost/variant/apply_visitor.hpp>

namespace stan {
  namespace lang {

    bool is_multi_index(const idx& idx) {
      is_multi_index_vis v;
      return boost::apply_visitor(v, idx.idx_);
    }

  }
}
#endif
