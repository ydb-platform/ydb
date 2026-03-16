#ifndef STAN_MATH_PRIM_SCAL_META_BROADCAST_ARRAY_HPP
#define STAN_MATH_PRIM_SCAL_META_BROADCAST_ARRAY_HPP

#include <stdexcept>

namespace stan {
namespace math {
namespace internal {
template <typename T>
class broadcast_array {
 private:
  T& prim_;

 public:
  explicit broadcast_array(T& prim) : prim_(prim) {}

  T& operator[](int /*i*/) { return prim_; }

  /**
   * We can assign any right hand side which allows for indexing to a
   * broadcast_array. The idea is that the entry for the first index is what
   * gets assigned. The most common use-case should be where the rhs is some
   * container of length 1.
   */
  template <typename Y>
  void operator=(const Y& m) {
    prim_ = m[0];
  }
};

template <typename T, typename S>
class empty_broadcast_array {
 public:
  empty_broadcast_array() {}
  /**
   * Not implemented so cannot be called.
   */
  T& operator[](int /*i*/);

  /**
   * Not implemented so cannot be called.
   */
  template <typename Y>
  void operator=(const Y& /*A*/);
};
}  // namespace internal
}  // namespace math
}  // namespace stan

#endif
