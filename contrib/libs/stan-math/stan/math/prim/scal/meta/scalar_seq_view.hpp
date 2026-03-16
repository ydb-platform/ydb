#ifndef STAN_MATH_PRIM_SCAL_META_SCALAR_SEQ_VIEW_HPP
#define STAN_MATH_PRIM_SCAL_META_SCALAR_SEQ_VIEW_HPP

#include <stan/math/prim/scal/meta/scalar_type.hpp>

namespace stan {
/**
 * scalar_seq_view provides a uniform sequence-like wrapper around either a
 * scalar or a sequence of scalars.
 *
 * @tparam C the container type; will be the scalar type if wrapping a scalar
 * @tparam T the scalar type
 */
template <typename C, typename T = typename scalar_type<C>::type>
class scalar_seq_view {
 public:
  explicit scalar_seq_view(const C& c) : c_(c) {}

  /**
   * Segfaults if out of bounds.
   * @param i index
   * @return the element at the specified position in the container
   */
  const T& operator[](int i) const { return c_[i]; }

  int size() const { return c_.size(); }

 private:
  const C& c_;
};

/**
 * This specialization handles wrapping a scalar as if it were a sequence.
 *
 * @tparam T the scalar type
 */
template <typename T>
class scalar_seq_view<T, T> {
 public:
  explicit scalar_seq_view(const T& t) : t_(t) {}

  const T& operator[](int /* i */) const { return t_; }

  int size() const { return 1; }

 private:
  const T& t_;
};
}  // namespace stan
#endif
