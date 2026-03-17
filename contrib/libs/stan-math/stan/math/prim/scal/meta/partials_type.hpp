#ifndef STAN_MATH_PRIM_SCAL_META_PARTIALS_TYPE_HPP
#define STAN_MATH_PRIM_SCAL_META_PARTIALS_TYPE_HPP

namespace stan {

template <typename T>
struct partials_type {
  typedef T type;
};

}  // namespace stan
#endif
