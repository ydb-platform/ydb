#ifndef STAN_LANG_AST_FUN_NUM_INDEX_OP_DIMS_DEF_HPP
#define STAN_LANG_AST_FUN_NUM_INDEX_OP_DIMS_DEF_HPP

#include <stan/lang/ast.hpp>
#include <vector>

namespace stan {
namespace lang {

size_t num_index_op_dims(const std::vector<std::vector<expression> >& dimss) {
  size_t total = 0U;
  for (size_t i = 0; i < dimss.size(); ++i)
    total += dimss[i].size();
  return total;
}

}  // namespace lang
}  // namespace stan
#endif
