#ifndef STAN_MATH_FWD_SCAL_META_PARTIALS_TYPE_HPP
#define STAN_MATH_FWD_SCAL_META_PARTIALS_TYPE_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/meta/partials_type.hpp>

namespace stan {

template <typename T>
struct partials_type<stan::math::fvar<T> > {
  typedef T type;
};

}  // namespace stan
#endif
