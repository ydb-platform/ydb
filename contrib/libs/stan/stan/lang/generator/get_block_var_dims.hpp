#ifndef STAN_LANG_GENERATOR_GET_BLOCK_VAR_DIMS_HPP
#define STAN_LANG_GENERATOR_GET_BLOCK_VAR_DIMS_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <ostream>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Return vector of size expressions for all dimensions
     * of a block_var_decl in the following order:
     * matrix cols (if matrix type),
     * matrix row / row_vector / vector length (if matrix/vec type),
     * array dim N through array dim 1
     *
     * @param[in] decl block_var_decl
     */
    std::vector<expression>
    get_block_var_dims(const block_var_decl decl) {
      std::vector<expression> dims;

      block_var_type bt = decl.type().innermost_type();
      if (bt.bare_type().is_matrix_type()) {
        dims.push_back(bt.arg2());
        dims.push_back(bt.arg1());
      } else if (bt.bare_type().is_row_vector_type()
                 || bt.bare_type().is_vector_type()) {
        dims.push_back(bt.arg1());
      }
      std::vector<expression> ar_lens = decl.type().array_lens();
      for (size_t i = ar_lens.size(); i-- > 0; ) {
        dims.push_back(ar_lens[i]);
      }
      return dims;
    }

  }
}
#endif
