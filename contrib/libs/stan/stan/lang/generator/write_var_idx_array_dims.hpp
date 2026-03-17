#ifndef STAN_LANG_GENERATOR_WRITE_VAR_IDX_ARRAY_DIMS_HPP
#define STAN_LANG_GENERATOR_WRITE_VAR_IDX_ARRAY_DIMS_HPP

#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the loop indexes for the specified variable
     * which has the specified number of array dimensions,
     * writing to the specified stream.
     *
     * @param[in] num_ar_dims number of array dimensions of variable
     * @param[in,out] o stream for generating
     */
    void write_var_idx_array_dims(size_t num_ar_dims,
                                  std::ostream& o) {
      for (size_t i = 0; i < num_ar_dims; ++i)
        o << "[i_" << i << "__]";
    }

  }
}
#endif
