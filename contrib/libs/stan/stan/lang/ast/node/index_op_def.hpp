#ifndef STAN_LANG_AST_NODE_INDEX_OP_DEF_HPP
#define STAN_LANG_AST_NODE_INDEX_OP_DEF_HPP

#include <stan/lang/ast.hpp>
#include <vector>

namespace stan {
namespace lang {

index_op::index_op() {}

index_op::index_op(const expression& expr,
                   const std::vector<std::vector<expression> >& dimss)
    : expr_(expr), dimss_(dimss) {
  infer_type();
}

void index_op::infer_type() {
  size_t total = 0U;
  for (size_t i = 0; i < dimss_.size(); ++i)
    total += dimss_[i].size();
  type_ = infer_type_indexing(expr_.bare_type(), total);
}

}  // namespace lang
}  // namespace stan
#endif
