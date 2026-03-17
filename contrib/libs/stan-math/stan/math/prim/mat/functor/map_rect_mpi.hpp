#ifdef STAN_MPI

#ifndef STAN_MATH_PRIM_MAT_FUNCTOR_MAP_RECT_MPI_HPP
#define STAN_MATH_PRIM_MAT_FUNCTOR_MAP_RECT_MPI_HPP

#include <stan/math/prim/mat/functor/map_rect_concurrent.hpp>
#include <stan/math/prim/arr/functor/mpi_distributed_apply.hpp>
#include <stan/math/prim/mat/functor/map_rect_reduce.hpp>
#include <stan/math/prim/mat/functor/map_rect_combine.hpp>
#include <stan/math/prim/mat/functor/mpi_parallel_call.hpp>

#include <vector>

namespace stan {
namespace math {
namespace internal {

template <int call_id, typename F, typename T_shared_param,
          typename T_job_param>
Eigen::Matrix<typename stan::return_type<T_shared_param, T_job_param>::type,
              Eigen::Dynamic, 1>
map_rect_mpi(
    const Eigen::Matrix<T_shared_param, Eigen::Dynamic, 1>& shared_params,
    const std::vector<Eigen::Matrix<T_job_param, Eigen::Dynamic, 1>>&
        job_params,
    const std::vector<std::vector<double>>& x_r,
    const std::vector<std::vector<int>>& x_i, std::ostream* msgs = nullptr) {
  typedef internal::map_rect_reduce<F, T_shared_param, T_job_param> ReduceF;
  typedef internal::map_rect_combine<F, T_shared_param, T_job_param> CombineF;

  // whenever the cluster is already busy with some command we fall
  // back to serial execution (possible if map_rect calls are nested
  // or MPI facility used already in use)
  try {
    mpi_parallel_call<call_id, ReduceF, CombineF> job_chunk(
        shared_params, job_params, x_r, x_i);

    return job_chunk.reduce_combine();
  } catch (const mpi_is_in_use& e) {
    return map_rect_concurrent<call_id, F>(shared_params, job_params, x_r, x_i,
                                           msgs);
  }
}
}  // namespace internal
}  // namespace math
}  // namespace stan

#define STAN_REGISTER_MPI_MAP_RECT(CALLID, FUNCTOR, SHARED, JOB)               \
  namespace stan {                                                             \
  namespace math {                                                             \
  namespace internal {                                                         \
  typedef FUNCTOR mpi_mr_##CALLID##_##SHARED##_##JOB##_;                       \
  typedef map_rect_reduce<mpi_mr_##CALLID##_##SHARED##_##JOB##_, SHARED, JOB>  \
      mpi_mr_##CALLID##_##SHARED##_##JOB##_red_;                               \
  typedef map_rect_combine<mpi_mr_##CALLID##_##SHARED##_##JOB##_, SHARED, JOB> \
      mpi_mr_##CALLID##_##SHARED##_##JOB##_comb_;                              \
  typedef mpi_parallel_call<CALLID, mpi_mr_##CALLID##_##SHARED##_##JOB##_red_, \
                            mpi_mr_##CALLID##_##SHARED##_##JOB##_comb_>        \
      mpi_mr_##CALLID##_##SHARED##_##JOB##_pcall_;                             \
  }                                                                            \
  }                                                                            \
  }                                                                            \
  STAN_REGISTER_MPI_DISTRIBUTED_APPLY(                                         \
      stan::math::internal::mpi_mr_##CALLID##_##SHARED##_##JOB##_pcall_)

#define STAN_REGISTER_MPI_MAP_RECT_ALL(CALLID, FUNCTOR) \
  STAN_REGISTER_MPI_MAP_RECT(CALLID, FUNCTOR, double, double)

// redefine register macro to use MPI variant
#undef STAN_REGISTER_MAP_RECT
#define STAN_REGISTER_MAP_RECT(CALLID, FUNCTOR) \
  STAN_REGISTER_MPI_MAP_RECT_ALL(CALLID, FUNCTOR)

#endif

#endif
