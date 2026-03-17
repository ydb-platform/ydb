#ifndef STAN_LANG_AST_NODE_INDEX_OP_SLICED_DEF_HPP
#define STAN_LANG_AST_NODE_INDEX_OP_SLICED_DEF_HPP

#include <stan/lang/ast.hpp>
#include <vector>

namespace stan {
  namespace lang {

    index_op_sliced::index_op_sliced() { }

    index_op_sliced::index_op_sliced(const expression& expr,
                                     const std::vector<idx>& idxs)
      : expr_(expr), idxs_(idxs), type_(indexed_type(expr_, idxs_)) { }

    void index_op_sliced::infer_type() {
      type_ = indexed_type(expr_, idxs_);
    }

  }
}
#endif
