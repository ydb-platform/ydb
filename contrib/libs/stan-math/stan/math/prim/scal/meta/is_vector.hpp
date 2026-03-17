#ifndef STAN_MATH_PRIM_SCAL_META_IS_VECTOR_HPP
#define STAN_MATH_PRIM_SCAL_META_IS_VECTOR_HPP

namespace stan {

// FIXME: use boost::type_traits::remove_all_extents to
//        extend to array/ptr types

template <typename T>
struct is_vector {
  enum { value = 0 };
  typedef T type;
};
}  // namespace stan
#endif
