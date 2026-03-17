#ifndef STAN_MATH_PRIM_SCAL_META_ERROR_INDEX_HPP
#define STAN_MATH_PRIM_SCAL_META_ERROR_INDEX_HPP

namespace stan {

struct error_index {
  enum {
    value =
#ifdef ERROR_INDEX
        ERROR_INDEX
#else
        1
#endif
  };
};

}  // namespace stan
#endif
