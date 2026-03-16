#include <stan/math/prim/scal/meta/broadcast_array.hpp>
#include <Eigen/Dense>
#include <stdexcept>

#ifndef STAN_MATH_PRIM_MAT_META_BROADCAST_ARRAY_HPP
#define STAN_MATH_PRIM_MAT_META_BROADCAST_ARRAY_HPP

namespace stan {
namespace math {
namespace internal {
template <typename ViewElt, typename OpElt, int R, int C>
class empty_broadcast_array<ViewElt, Eigen::Matrix<OpElt, R, C> > {
 public:
  empty_broadcast_array() {}
  /**
   * Not implemented so cannot be called.
   */
  ViewElt& operator[](int /*i*/);
  /**
   * Not implemented so cannot be called.
   */
  ViewElt& operator()(int /*i*/);
  /**
   * Not implemented so cannot be called.
   */
  void operator=(const Eigen::Matrix<ViewElt, R, C>& /*A*/);
  /**
   * Not implemented so cannot be called.
   */
  void operator+=(Eigen::Matrix<ViewElt, R, C> /*A*/);
  /**
   * Not implemented so cannot be called.
   */
  void operator-=(Eigen::Matrix<ViewElt, R, C> /*A*/);
  /**
   * Not implemented so cannot be called.
   */
  Eigen::Matrix<ViewElt, 1, C>& row(int /*i*/);
  /**
   * Not implemented so cannot be called.
   */
  Eigen::Matrix<ViewElt, R, 1>& col(int /*i*/);
};
}  // namespace internal
}  // namespace math
}  // namespace stan
#endif
