#ifndef STAN_LANG_AST_FUN_IS_MULTI_INDEX_VIS_DEF_HPP
#define STAN_LANG_AST_FUN_IS_MULTI_INDEX_VIS_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    is_multi_index_vis::is_multi_index_vis() { }

    bool is_multi_index_vis::operator()(const uni_idx& i) const {
      return false;
    }

    bool is_multi_index_vis::operator()(const multi_idx& i) const {
      return true;
    }

    bool is_multi_index_vis::operator()(const omni_idx& i) const {
      return true;
    }

    bool is_multi_index_vis::operator()(const lb_idx& i) const {
      return true;
    }

    bool is_multi_index_vis::operator()(const ub_idx& i) const {
      return true;
    }

    bool is_multi_index_vis::operator()(const lub_idx& i) const {
      return true;
    }

  }
}
#endif
