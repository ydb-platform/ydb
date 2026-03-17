#ifndef STAN_MATH_FWD_MAT_FUN_TYPEDEFS_HPP
#define STAN_MATH_FWD_MAT_FUN_TYPEDEFS_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/fwd/core.hpp>
#include <stan/math/fwd/mat/fun/Eigen_NumTraits.hpp>

namespace stan {
namespace math {

typedef Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic>::Index size_type;

typedef Eigen::Matrix<fvar<double>, Eigen::Dynamic, Eigen::Dynamic> matrix_fd;

typedef Eigen::Matrix<fvar<fvar<double> >, Eigen::Dynamic, Eigen::Dynamic>
    matrix_ffd;

typedef Eigen::Matrix<fvar<double>, Eigen::Dynamic, 1> vector_fd;

typedef Eigen::Matrix<fvar<fvar<double> >, Eigen::Dynamic, 1> vector_ffd;

typedef Eigen::Matrix<fvar<double>, 1, Eigen::Dynamic> row_vector_fd;

typedef Eigen::Matrix<fvar<fvar<double> >, 1, Eigen::Dynamic> row_vector_ffd;

}  // namespace math
}  // namespace stan
#endif
