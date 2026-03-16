#ifndef STAN_MATH_PRIM_MAT_FUNCTOR_MAP_RECT_REDUCE_HPP
#define STAN_MATH_PRIM_MAT_FUNCTOR_MAP_RECT_REDUCE_HPP

#include <stan/math/prim/mat/fun/typedefs.hpp>

#include <vector>

namespace stan {
namespace math {
namespace internal {

/* Base template class for the reduce step of map_rect.
 *
 * This class wraps the user functor F which is executed with a shared
 * parameter vector and job specific parameters, real and int data.
 *
 * The class exposes a double only signature for all inputs while the
 * template parameters determine what the client code is actually
 * expecting to be calculated. So whenever T_shared_param or/and
 * T_job_param correspond to an autodiff type then the respective
 * gradients are calculated.
 *
 * The defined functor always returns a matrix of type double. Each
 * column correspond to an output of the function which can return
 * multiple outputs per given input. The rows of this returned matrix
 * contain the gradients wrt to the shared and/or job specific
 * parameters (in this order).
 *
 * No higher order output format is defined yet.
 *
 * @tparam F user functor
 * @tparam T_shared_param type of shared parameters
 * @tparam T_job_param type of job specific parameters
 */
template <typename F, typename T_shared_param, typename T_job_param>
class map_rect_reduce {};

template <typename F>
class map_rect_reduce<F, double, double> {
 public:
  matrix_d operator()(const vector_d& shared_params,
                      const vector_d& job_specific_params,
                      const std::vector<double>& x_r,
                      const std::vector<int>& x_i,
                      std::ostream* msgs = nullptr) const {
    return F()(shared_params, job_specific_params, x_r, x_i, msgs).transpose();
  }
};

}  // namespace internal
}  // namespace math
}  // namespace stan

#endif
