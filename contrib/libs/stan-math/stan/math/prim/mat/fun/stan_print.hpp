#ifndef STAN_MATH_PRIM_MAT_FUN_STAN_PRINT_HPP
#define STAN_MATH_PRIM_MAT_FUN_STAN_PRINT_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <vector>

namespace stan {
namespace math {
// prints used in generator for print() statements in modeling language

template <typename T>
void stan_print(std::ostream* o, const T& x) {
  *o << x;
}

template <typename T>
void stan_print(std::ostream* o, const std::vector<T>& x) {
  *o << '[';
  for (size_t i = 0; i < x.size(); ++i) {
    if (i > 0)
      *o << ',';
    stan_print(o, x[i]);
  }
  *o << ']';
}

template <typename T>
void stan_print(std::ostream* o, const Eigen::Matrix<T, Eigen::Dynamic, 1>& x) {
  *o << '[';
  for (int i = 0; i < x.size(); ++i) {
    if (i > 0)
      *o << ',';
    stan_print(o, x(i));
  }
  *o << ']';
}

template <typename T>
void stan_print(std::ostream* o, const Eigen::Matrix<T, 1, Eigen::Dynamic>& x) {
  *o << '[';
  for (int i = 0; i < x.size(); ++i) {
    if (i > 0)
      *o << ',';
    stan_print(o, x(i));
  }
  *o << ']';
}

template <typename T>
void stan_print(std::ostream* o,
                const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& x) {
  *o << '[';
  for (int i = 0; i < x.rows(); ++i) {
    if (i > 0)
      *o << ',';
    *o << '[';
    for (int j = 0; j < x.row(i).size(); ++j) {
      if (j > 0)
        *o << ',';
      stan_print(o, x.row(i)(j));
    }
    *o << ']';
  }
  *o << ']';
}

}  // namespace math
}  // namespace stan
#endif
