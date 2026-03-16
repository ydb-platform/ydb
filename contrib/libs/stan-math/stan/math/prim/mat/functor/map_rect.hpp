#ifndef STAN_MATH_PRIM_MAT_FUNCTOR_MAP_RECT_HPP
#define STAN_MATH_PRIM_MAT_FUNCTOR_MAP_RECT_HPP

#include <stan/math/prim/arr/err/check_matching_sizes.hpp>
#include <stan/math/prim/mat/fun/dims.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>

#define STAN_REGISTER_MAP_RECT(CALLID, FUNCTOR)

#ifdef STAN_MPI
#include <stan/math/prim/mat/functor/map_rect_mpi.hpp>
#else
#include <stan/math/prim/mat/functor/map_rect_concurrent.hpp>
#endif

#include <vector>

namespace stan {
namespace math {

/**
 * Map N function evaluations to parameters and data which are in
 * rectangular format. Each function evaluation may return a column
 * vector of different sizes and the output is the concatenated vector
 * from all evaluations.
 *
 * In addition to job specific parameters, real and int data, a shared
 * parameter vector is repeated in all evaluations. All input
 * parameters are stored as vectors whereas data is stored as arrays.
 *
 * For N jobs the output of this function is
 *
 * [
 * f(shared_params, job_params[1], x_r[1], x_i[1]),
 * f(shared_params, job_params[2], x_r[2], x_i[2]),
 * ... ]'.
 *
 * The function is implemented with serial execution and with
 * parallelism using threading or MPI (TODO). The threading version is
 * used if the compiler flag STAN_THREADS is set during compilation
 * while the MPI version is only available if STAN_MPI is defined. The
 * MPI parallelism takes precedence over serial or threading execution
 * of the function.
 *
 * For the threaded parallelism the N jobs are chunked into T blocks
 * which are executed asynchronously using the async C++11
 * facility. This ensure that at most T threads are used, but the
 * actual number of threads is controlled by the implementation of
 * async provided by the compiler. Note that nested calls of map_rect
 * will lead to a multiplicative increase in the number of job chunks
 * generated. The number of threads T is controlled at runtime via the
 * STAN_NUM_threads environment variable, see the get_num_threads
 * function for details.
 *
 * For the MPI version to work this function has these special
 * non-standard conventions:
 *
 * - The call_id template parameter is considered as a label for the
 *   functor F and data combination. Since MPI communication is
 *   expensive, the real and int data is transmitted only a single
 *   time per functor F / call_id combination to the workers.
 * - The MPI implementation requires that the functor type fully
 *   specifies the functor and hence requires a default constructible
 *   function object. This choice reduces the need for communication
 *   across MPI as the type is sufficient and the state of the
 *   functor is not necessary to transmit. Thus, the functor is
 *   specified as template argument only.
 * - The size of the returned vector of each job must stay consistent
 *   when performing repeated calls to map_rect (which usually vary
 *   the values of the parameters).
 * - To achieve the exact same results between the serial and the MPI
 *   evaluation scheme both variants work in exactly the same way
 *   which is to use nested AD calls and pre-computing all gradients
 *   when the function gets called. The usual evaluation scheme would
 *   build an AD graph instead of doing on-the-spot gradient
 *   evaluation. For large problems this results in speedups for the
 *   serial version even on a single core due to smaller AD graph
 *   sizes.
 * - In MPI operation mode, no outputs from the workers are streamed
 *   back to the root.
 * - Finally, each map_rect call must be registered with the
 *   STAN_REGISTER_MAP_RECT macro. This is required to enable
 *   communication between processes for the MPI case. The macro
 *   definition is empty if MPI is not enabled.
 *
 * The functor F is expected to have the usual operator() function
 * with signature
 *
 * template <typename T1, typename T2>
 * Eigen::Matrix<typename stan::return_type<T1, T2>::type, Eigen::Dynamic, 1>
 * operator()(const Eigen::Matrix<T1, Eigen::Dynamic, 1>& eta,
 *            const Eigen::Matrix<T2, Eigen::Dynamic, 1>& theta,
 *            const std::vector<double>& x_r, const std::vector<int>& x_i,
 *            std::ostream* msgs = 0) const { ... }
 *
 * WARNING: For the MPI case, the data arguments are NOT checked if
 * they are unchanged between repeated evaluations for a given
 * call_id/functor F pair. This is silently assumed to be immutable
 * between evaluations.
 *
 * @tparam T_shared_param Type of shared parameters.
 * @tparam T_job_param Type of job specific parameters.
 * @param shared_params shared parameter vector passed as first
 * argument to functor for all jobs
 * @param job_params Array of job specific parameter vectors. All job
 * specific parameters must have matching sizes.
 * @param x_r Array of real arrays for each job. The first dimension
 * must match the number of jobs (which is determined by the first
 * dimension of job_params) and each entry must have the same size.
 * @param x_i Array of int data with the same conventions as x_r.
 * @param msgs Output stream for messages.
 * @tparam call_id Label for functor/data combination. See above for
 * details.
 * @tparam F Functor which is applied to all job specific parameters
 * with conventions described.
 * @return concatenated results from all jobs
 */

template <int call_id, typename F, typename T_shared_param,
          typename T_job_param>
Eigen::Matrix<typename stan::return_type<T_shared_param, T_job_param>::type,
              Eigen::Dynamic, 1>
map_rect(const Eigen::Matrix<T_shared_param, Eigen::Dynamic, 1>& shared_params,
         const std::vector<Eigen::Matrix<T_job_param, Eigen::Dynamic, 1>>&
             job_params,
         const std::vector<std::vector<double>>& x_r,
         const std::vector<std::vector<int>>& x_i,
         std::ostream* msgs = nullptr) {
  static const char* function = "map_rect";
  typedef Eigen::Matrix<
      typename stan::return_type<T_shared_param, T_job_param>::type,
      Eigen::Dynamic, 1>
      return_t;

  check_matching_sizes(function, "job parameters", job_params, "real data",
                       x_r);
  check_matching_sizes(function, "job parameters", job_params, "int data", x_i);

  // check size consistency of inputs per job
  const std::vector<int> job_params_dims = dims(job_params);
  const int size_job_params = job_params_dims[1];
  const int size_x_r = dims(x_r)[1];
  const int size_x_i = dims(x_i)[1];
  for (int i = 1; i < job_params_dims[0]; i++) {
    check_size_match(function,
                     "Size of one of the vectors of "
                     "the job specific parameters",
                     job_params[i].size(),
                     "size of another vector of the "
                     "job specifc parameters",
                     size_job_params);
    check_size_match(function,
                     "Size of one of the arrays of "
                     "the job specific real data",
                     x_r[i].size(),
                     "size of another array of the "
                     "job specifc real data",
                     size_x_r);
    check_size_match(function,
                     "Size of one of the arrays of "
                     "the job specific int data",
                     x_i[i].size(),
                     "size of another array of the "
                     "job specifc int data",
                     size_x_i);
  }

  if (job_params_dims[0] == 0)
    return return_t();

#ifdef STAN_MPI
  return internal::map_rect_mpi<call_id, F, T_shared_param, T_job_param>(
      shared_params, job_params, x_r, x_i, msgs);
#else
  return internal::map_rect_concurrent<call_id, F, T_shared_param, T_job_param>(
      shared_params, job_params, x_r, x_i, msgs);
#endif
}

}  // namespace math
}  // namespace stan

#endif
