#ifndef STAN_MATH_PRIM_MAT_META_LENGTH_MVT_HPP
#define STAN_MATH_PRIM_MAT_META_LENGTH_MVT_HPP

#include <stan/math/prim/scal/meta/length_mvt.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stdexcept>
#include <vector>

namespace stan {

template <typename T, int R, int C>
size_t length_mvt(const Eigen::Matrix<T, R, C>& /* unused */) {
  return 1U;
}

template <typename T, int R, int C>
size_t length_mvt(const std::vector<Eigen::Matrix<T, R, C> >& x) {
  return x.size();
}

}  // namespace stan
#endif
