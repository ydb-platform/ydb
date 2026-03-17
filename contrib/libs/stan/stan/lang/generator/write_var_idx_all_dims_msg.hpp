#ifndef STAN_LANG_GENERATOR_WRITE_VAR_IDX_ALL_DIMS_MSG_HPP
#define STAN_LANG_GENERATOR_WRITE_VAR_IDX_ALL_DIMS_MSG_HPP

#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the loop indexes for the specified variable
     * which has the specified number of array dimensions and
     * row/column arguments, writing to the specified stream.
     * 
     * Regardless of indexing order, indexes on variable are always in-order.
     * e.g., 3-d array of matrices is indexed: [d1][d2][d3](row,col)
     *
     * @param[in] num_ar_dims number of array dimensions of variable
     * @param[in] num_args ternary indicator for matrix/vector/scalar types
     * @param[in,out] o stream for generating
     */
    void write_var_idx_all_dims_msg(size_t num_ar_dims,
                                    size_t num_args,
                                    std::ostream& o) {
      for (size_t i = 0; i < num_ar_dims; ++i)
        o << " << \"[\" << k_" << i << "__ << \"]\"";
      if (num_args == 1)
        o << " << \"(\" << j_1__ << \")\"";
      else if (num_args == 2)
        o << " << \"(\" << j_1__ << \", \" << j_2__ << \")\"";
    }

  }
}
#endif
