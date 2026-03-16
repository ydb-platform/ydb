#ifndef STAN_LANG_AST_FUN_NUM_INDEX_OP_DIMS_HPP
#define STAN_LANG_AST_FUN_NUM_INDEX_OP_DIMS_HPP

#include <cstddef>
#include <vector>

namespace stan {
  namespace lang {

    struct expression;

    /**
     * Return the total number of index_op dimensions when the specified
     * vectors of expressions are concatenated.
     *
     * @param dimss vector of vector of dimension expressions
     */
    std::size_t num_index_op_dims(const std::vector<std::vector<expression> >&
                                  dimss);

  }
}
#endif
