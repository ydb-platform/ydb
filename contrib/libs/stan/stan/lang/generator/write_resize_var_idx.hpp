#ifndef STAN_LANG_GENERATOR_WRITE_RESIZE_VAR_IDX_HPP
#define STAN_LANG_GENERATOR_WRITE_RESIZE_VAR_IDX_HPP

#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the loop indexes for the first n-1 of the specified
     * number of array dimensions, writing to the specified stream.
     *
     * @param[in] num_ar_dims number of array dimensions of variable
     * @param[in,out] o stream for generating
     */
    void write_resize_var_idx(size_t num_ar_dims,
                              std::ostream& o) {
      for (size_t i = 0; i < num_ar_dims - 1; ++i)
        o << "[d_" << i << "__]";
    }

  }
}
#endif
