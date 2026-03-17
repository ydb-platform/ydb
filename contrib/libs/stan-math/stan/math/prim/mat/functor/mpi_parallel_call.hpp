#ifdef STAN_MPI

#ifndef STAN_MATH_PRIM_MAT_FUNCTOR_MPI_PARALLEL_CALL_HPP
#define STAN_MATH_PRIM_MAT_FUNCTOR_MPI_PARALLEL_CALL_HPP

#include <stan/math/prim/arr/functor/mpi_cluster.hpp>
#include <stan/math/prim/arr/functor/mpi_distributed_apply.hpp>
#include <stan/math/prim/mat/fun/to_array_1d.hpp>
#include <stan/math/prim/mat/fun/dims.hpp>

#include <mutex>
#include <algorithm>
#include <vector>
#include <type_traits>
#include <functional>

namespace stan {
namespace math {

namespace internal {

/**
 * Container for locally cached data which is essentially implemented
 * as singleton. That is, the static data is labelled by the user with
 * call_id which forms a type in the program. As we have to associate
 * multiple data items with a given call_id the member integer is used
 * to allow for multiple items per call_id. Think of this as a
 * singleton per static data.
 *
 * The singleton must be initialized with data once using the store
 * method. After calling the store method the state of the object is
 * valid and the data can be read using the data function which
 * returns a const reference to the data.
 *
 * @tparam call_id label of static data defined by the user
 * @tparam member labels a specific data item for the call_id context
 * @tparam T the type of the object stored
 */
template <int call_id, int member, typename T>
class mpi_parallel_call_cache {
  static T local_;
  static bool is_valid_;

 public:
  typedef const T cache_t;

  mpi_parallel_call_cache() = delete;
  mpi_parallel_call_cache(const mpi_parallel_call_cache<call_id, member, T>&)
      = delete;
  mpi_parallel_call_cache& operator=(
      const mpi_parallel_call_cache<call_id, member, T>&)
      = delete;

  /**
   * Query if cache is in valid which it is once data has been stored.
   * @return flag if cache has been initialized.
   */
  static bool is_valid() { return is_valid_; }

  /**
   * Store data to be cached locally. Must be called a single time
   * only. Once data is stored the cache is marked as valid. As there
   * is no within-process parallelism no locking is used here.
   */
  static void store(const T& data) {
    if (is_valid_)
      throw std::runtime_error("Cache can only store a single data item.");
    local_ = data;
    is_valid_ = true;
  }

  /**
   * Obtain const reference to locally cached data if cache is valid
   * (throws otherwise).
   * @return const reference to stored data of type T
   */
  static cache_t& data() {
    if (unlikely(!is_valid_))
      throw std::runtime_error("Cache not yet valid.");
    return local_;
  }
};

template <int call_id, int member, typename T>
T mpi_parallel_call_cache<call_id, member, T>::local_;

template <int call_id, int member, typename T>
bool mpi_parallel_call_cache<call_id, member, T>::is_valid_ = false;

}  // namespace internal

/**
 * The MPI parallel call class manages the distributed evaluation of a
 * collection of tasks following the map - reduce - combine
 * pattern. The class organizes the distribution of all job related
 * information from the root node to all worker nodes. The class
 * discriminates between parameters and static data. The static data
 * is only transmitted a single time and cached on each worker locally
 * after the inital transfer.
 *
 * The flow of commands are:
 *
 * 1. The constructor of this class must be called on the root node where
 *    all parameters and static data is passed to the class.
 * 2. The constructor then tries to allocate the MPI cluster ressource
 *    to obtain control over all workers in the cluster. If the
 *    cluster is locked already, then an exception is fired.
 * 3. The worker nodes are instructed to run the static
 *    distributed_apply method of this class. This static method then
 *    instantiates a mpi_parallel_call instance on the workers.
 * 4. The root then broadcasts and scatters all necessary data to the
 *    cluster. Static data (including meta information on data shapes)
 *    are locally cached such that static data is only transferred on
 *    the first evaluation. Note that the work is equally distributed
 *    among the workers. That is N jobs are distributed ot a cluster
 *    of size W in N/W chunks (the remainder is allocated to node 1
 *    onwards which ensures that the root node 0 has one job less).
 * 5. Once the parameters and static data is distributed, the reduce
 *    operation is applied per defined job. Each job is allowed to
 *    return a different number of outputs such that the resulting
 *    data structure is a ragged array. The ragged array structure
 *    becomes known to mpi_parallel_call during the first evaluation
 *    and must not change for future calls.
 * 6. Finally the local results are gathered on the root node over MPI
 *    and given on the root node to the combine functor along
 *    with the ragged array data structure.
 *
 * The MPI cluster resource is aquired with construction of
 * mpi_parallel_call and is freed once the mpi_parallel_call goes out
 * of scope (that is, deconstructed).
 *
 * Note 1: During MPI operation everything must run synchronous. That
 * is, if a job fails on any of the workers then the execution must
 * still continue on all other workers. In order to maintain a
 * synchronized state, even the worker with a failed job must return a
 * valid output chunk since the gather commands issued on the root to
 * collect results would otherwise fail. Thus, if a job fails on any
 * worker the a respective status flag will be transferred after the
 * reduce and the results gather step.
 *
 * Note 2: During the first evaluation of the function the ragged
 * array sizes need to be collected on the root from all workers. This
 * is needed on the root such that the root knows how many outputs are
 * computed on each worker. This information is then cached for all
 * subsequent evaluations. However, caching this information can
 * occur if and only if the evaluation of all functions was
 * successfull on all workers during the first run. Thus, the first
 * evaluation is handled with special care to ensure that caching of
 * this meta info is only done when all workers have successfully
 * evaluated the function and otherwise an exception is raised.
 *
 * @tparam call_id label for the static data
 * @tparam ReduceF reduce function called for each job, \see
 * internal::map_rect_reduce
 * @tparam CombineF combine function called on the combined results on
 * each job along with the ragged data structure information, \see
 * internal::map_rect_combine
 */
template <int call_id, typename ReduceF, typename CombineF>
class mpi_parallel_call {
  boost::mpi::communicator world_;
  const std::size_t rank_ = world_.rank();
  const std::size_t world_size_ = world_.size();
  std::unique_lock<std::mutex> cluster_lock_;

  typedef typename CombineF::result_t result_t;

  // local caches which hold local slices of data
  typedef internal::mpi_parallel_call_cache<call_id, 1,
                                            std::vector<std::vector<double>>>
      cache_x_r;
  typedef internal::mpi_parallel_call_cache<call_id, 2,
                                            std::vector<std::vector<int>>>
      cache_x_i;
  typedef internal::mpi_parallel_call_cache<call_id, 3, std::vector<int>>
      cache_f_out;
  typedef internal::mpi_parallel_call_cache<call_id, 4, std::vector<int>>
      cache_chunks;

  // # of outputs for given call_id+ReduceF+CombineF case
  static int num_outputs_per_job_;

  CombineF combine_;

  vector_d local_shared_params_dbl_;
  matrix_d local_job_params_dbl_;

 public:
  /**
   * Initiates a parallel MPI call on the root. The constructor
   * allocates the MPI ressource and initiates on all workers the MPI
   * parallel call which mirror the communication and execute a chunk
   * of the overall work.
   *
   * @tparam T_shared_param type of shared parameters
   * @tparam T_job_param type of job-specific parameters
   * @param shared_params shared parameter vector
   * @param job_params array of job-specifc parameter vectors
   * @param x_r array of job-specifc real arrays (data only argument)
   * @param x_i array of job-specifc int arrays (data only argument)
   */
  template <typename T_shared_param, typename T_job_param>
  mpi_parallel_call(
      const Eigen::Matrix<T_shared_param, Eigen::Dynamic, 1>& shared_params,
      const std::vector<Eigen::Matrix<T_job_param, Eigen::Dynamic, 1>>&
          job_params,
      const std::vector<std::vector<double>>& x_r,
      const std::vector<std::vector<int>>& x_i)
      : combine_(shared_params, job_params) {
    if (rank_ != 0)
      throw std::runtime_error(
          "problem sizes may only be defined on the root.");

    check_matching_sizes("mpi_parallel_call", "job parameters", job_params,
                         "continuous data", x_r);
    check_matching_sizes("mpi_parallel_call", "job parameters", job_params,
                         "integer data", x_i);

    if (cache_chunks::is_valid()) {
      int cached_num_jobs = sum(cache_chunks::data());
      check_size_match("mpi_parallel_call", "cached number of jobs",
                       cached_num_jobs, "number of jobs", job_params.size());
    }

    // make children aware of upcoming job & obtain cluster lock
    cluster_lock_ = mpi_broadcast_command<stan::math::mpi_distributed_apply<
        mpi_parallel_call<call_id, ReduceF, CombineF>>>();

    const std::vector<int> job_dims = dims(job_params);

    const size_type num_jobs = job_dims[0];
    const size_type num_job_params = num_jobs == 0 ? 0 : job_dims[1];

    const vector_d shared_params_dbl = value_of(shared_params);
    matrix_d job_params_dbl(num_job_params, num_jobs);

    for (int j = 0; j < num_jobs; ++j)
      job_params_dbl.col(j) = value_of(job_params[j]);

    setup_call(shared_params_dbl, job_params_dbl, x_r, x_i);
  }

  // called on remote sites
  mpi_parallel_call() : combine_() {
    if (rank_ == 0)
      throw std::runtime_error("problem sizes must be defined on the root.");

    setup_call(vector_d(), matrix_d(), std::vector<std::vector<double>>(),
               std::vector<std::vector<int>>());
  }

  /**
   * Entry point on the workers for the mpi_parallel_call.
   */
  static void distributed_apply() {
    // call constructor for the remotes
    mpi_parallel_call<call_id, ReduceF, CombineF> job_chunk;

    job_chunk.reduce_combine();
  }

  /**
   * Once all data is distributed and cached the reduce_combine
   * evaluates all assigned function evaluations locally, transfers
   * all results back to the root and finally combines on the root all
   * results.
   */
  result_t reduce_combine() {
    const std::vector<int>& job_chunks = cache_chunks::data();
    const int num_jobs = sum(job_chunks);

    const int first_job
        = std::accumulate(job_chunks.begin(), job_chunks.begin() + rank_, 0);
    const int num_local_jobs = local_job_params_dbl_.cols();
    int local_outputs_per_job = num_local_jobs == 0 ? 0 : num_outputs_per_job_;
    matrix_d local_output(
        local_outputs_per_job == -1 ? 0 : local_outputs_per_job,
        num_local_jobs);
    std::vector<int> local_f_out(num_local_jobs, -1);

    typename cache_x_r::cache_t& local_x_r = cache_x_r::data();
    typename cache_x_i::cache_t& local_x_i = cache_x_i::data();

    // check if we know already output sizes
    if (cache_f_out::is_valid()) {
      typename cache_f_out::cache_t& f_out = cache_f_out::data();
      const int num_outputs
          = std::accumulate(f_out.begin() + first_job,
                            f_out.begin() + first_job + num_local_jobs, 0);
      local_output.resize(Eigen::NoChange, num_outputs);
    }

    int local_ok = 1;
    try {
      for (int i = 0, offset = 0; i < num_local_jobs;
           offset += local_f_out[i], ++i) {
        const matrix_d job_output
            = ReduceF()(local_shared_params_dbl_, local_job_params_dbl_.col(i),
                        local_x_r[i], local_x_i[i], 0);
        local_f_out[i] = job_output.cols();

        if (local_outputs_per_job == -1) {
          local_outputs_per_job = job_output.rows();
          // changing rows, but leave column size as is
          local_output.conservativeResize(local_outputs_per_job,
                                          Eigen::NoChange);
        }

        if (local_output.cols() < offset + local_f_out[i])
          // leave row size, but change columns size
          local_output.conservativeResize(Eigen::NoChange,
                                          2 * (offset + local_f_out[i]));

        local_output.block(0, offset, local_output.rows(), local_f_out[i])
            = job_output;
      }
    } catch (const std::exception& e) {
      // see note 1 above for an explanation why we do not rethrow
      // here, but mereley flag it to keep the cluster synchronized
      local_ok = 0;
    }

    // during first execution we distribute the output sizes from
    // local jobs to the root. This needs to be done for the number of
    // outputs of each result and the number of outputs per job.
    if (num_outputs_per_job_ == -1) {
      // before we can cache the sizes locally we must ensure
      // that no exception has been fired from any node. Hence,
      // share the info about the current status across all nodes.
      int cluster_status = 0;
      boost::mpi::reduce(world_, local_ok, cluster_status, std::plus<int>(), 0);
      bool all_ok = cluster_status == static_cast<int>(world_size_);
      boost::mpi::broadcast(world_, all_ok, 0);
      if (!all_ok) {
        // err out on the root
        if (rank_ == 0) {
          throw std::domain_error("MPI error on first evaluation.");
        }
        // and ensure on the workers that they return into their
        // listening state
        return result_t();
      }

      // execution was ok everywhere such that we can now distribute the
      // outputs per job as recorded on the root on the first execution
      boost::mpi::broadcast(world_, local_outputs_per_job, 0);
      num_outputs_per_job_ = local_outputs_per_job;

      // the f_out cache may not yet be synchronized which we can do
      // now given that this first execution was just fine
      if (!cache_f_out::is_valid()) {
        std::vector<int> world_f_out(num_jobs, 0);
        boost::mpi::gatherv(world_, local_f_out.data(), num_local_jobs,
                            world_f_out.data(), job_chunks, 0);
        // on the root we now have all sizes from all children. Copy
        // over the local sizes to the world vector on each local node
        // in order to cache this information locally.
        std::copy(local_f_out.begin(), local_f_out.end(),
                  world_f_out.begin() + first_job);
        cache_f_out::store(world_f_out);
      }
    }

    typename cache_f_out::cache_t& world_f_out = cache_f_out::data();

    // check that cached sizes are the same as just collected from
    // this evaluation
    for (int i = 0; i < num_local_jobs; ++i) {
      if (world_f_out[first_job + i] != local_f_out[i]) {
        // in case of a mismatch we mark so, but flag the error only
        // on the root, see note 1 above.
        local_ok = 0;
        break;
      }
    }

    const std::size_t size_world_f_out = sum(world_f_out);
    matrix_d world_result(num_outputs_per_job_, size_world_f_out);

    std::vector<int> chunks_result(world_size_, 0);
    for (std::size_t i = 0, k = 0; i != world_size_; ++i)
      for (int j = 0; j != job_chunks[i]; ++j, ++k)
        chunks_result[i] += world_f_out[k] * num_outputs_per_job_;

    // collect results on root
    boost::mpi::gatherv(world_, local_output.data(), chunks_result[rank_],
                        world_result.data(), chunks_result, 0);

    // let root know if all went fine everywhere
    int cluster_status = 0;
    boost::mpi::reduce(world_, local_ok, cluster_status, std::plus<int>(), 0);
    bool all_ok = cluster_status == static_cast<int>(world_size_);

    // on the workers all is done now.
    if (rank_ != 0)
      return result_t();

    // in case something went wrong we throw on the root
    if (!all_ok)
      throw std::domain_error("Error during MPI evaluation.");

    return combine_(world_result, world_f_out);
  }

 private:
  /**
   * Performs a cached scatter of a 2D array (nested std::vector). On the
   * first call the data on the root is scattered to all workers and
   * is stored in the cache locally. Each worker only recieves its
   * respective chunk and marks the cache as valid such that
   * any subsequent calls will immediatley return the cached
   * chunk. Thus, after calling this function it is guranteed that
   * on all workers a call to T_cache::data() will return the same
   * chunk within each process of the cluster.
   *
   * @tparam T_cache static data storage type for the 2D array
   * @param data 2D array to be scattered from the root and a dummy
   * argument on workers
   * @return 2D array chunk of the worker
   */
  template <typename T_cache>
  typename T_cache::cache_t& scatter_array_2d_cached(
      typename T_cache::cache_t& data) {
    // distribute data only if not in cache yet
    if (T_cache::is_valid()) {
      return T_cache::data();
    }

    // number of elements of each data item must be transferred to
    // the workers
    std::vector<int> data_dims = dims(data);
    data_dims.resize(2);

    boost::mpi::broadcast(world_, data_dims.data(), 2, 0);

    const std::vector<int> job_chunks = mpi_map_chunks(data_dims[0], 1);
    const std::vector<int> data_chunks
        = mpi_map_chunks(data_dims[0], data_dims[1]);

    auto flat_data = to_array_1d(data);
    decltype(flat_data) local_flat_data(data_chunks[rank_]);

    if (data_dims[0] * data_dims[1] > 0) {
      if (rank_ == 0) {
        boost::mpi::scatterv(world_, flat_data.data(), data_chunks,
                             local_flat_data.data(), 0);
      } else {
        boost::mpi::scatterv(world_, local_flat_data.data(), data_chunks[rank_],
                             0);
      }
    }

    std::vector<decltype(flat_data)> local_data;
    auto local_iter = local_flat_data.begin();
    for (int i = 0; i != job_chunks[rank_]; ++i) {
      typename T_cache::cache_t::value_type const data_elem(
          local_iter, local_iter + data_dims[1]);
      local_data.push_back(data_elem);
      local_iter += data_dims[1];
    }

    // finally we cache it locally
    T_cache::store(local_data);
    return T_cache::data();
  }

  /**
   * Performs a cached broadcast of a 1D array (std::vector). On the
   * first call the data on the root is broadcasted to all workers and
   * is stored in the cache locally. This marks the cache as valid and
   * any subsequent calls will immediatley return the cached
   * results. Thus, after calling this function it is guranteed that
   * on all workers a call to T_cache::data() will return the same
   * vector within each process of the cluster.
   *
   * @tparam T_cache static data storage type for the 1D array
   * @param data vector to be broadcasted from the root and a dummy
   * argument on workers
   * @return broadcasted vector originating from the root
   */
  template <typename T_cache>
  typename T_cache::cache_t& broadcast_array_1d_cached(
      typename T_cache::cache_t& data) {
    if (T_cache::is_valid()) {
      return T_cache::data();
    }

    std::size_t data_size = data.size();
    boost::mpi::broadcast(world_, data_size, 0);

    std::vector<typename T_cache::cache_t::value_type> local_data = data;
    local_data.resize(data_size);

    boost::mpi::broadcast(world_, local_data.data(), data_size, 0);
    T_cache::store(local_data);
    return T_cache::data();
  }

  /**
   * Broadcasts an Eigen vector to the cluster. Meta
   * information as the data shape is treated as static data and only
   * transferred on the first call and read from cache
   * subsequently. This static shape data is stored with the
   * meta_cache_id label.
   *
   * @tparam meta_cache_id item label for static shape data
   * @param data vector to be broadcasted from the root and a dummy
   * argument on workers
   * @return broadcasted vector originating from the root
   */
  template <int meta_cache_id>
  vector_d broadcast_vector(const vector_d& data) {
    typedef internal::mpi_parallel_call_cache<call_id, meta_cache_id,
                                              std::vector<size_type>>
        meta_cache;
    const std::vector<size_type>& data_size
        = broadcast_array_1d_cached<meta_cache>({data.size()});

    vector_d local_data = data;
    local_data.resize(data_size[0]);

    boost::mpi::broadcast(world_, local_data.data(), data_size[0], 0);

    return local_data;
  }

  /**
   * Scatters an Eigen matrix column wise over the cluster. Meta
   * information as the data shape is treated as static data and only
   * transferred on the first call and read from cache
   * subsequently. This static shape data is stored with the
   * meta_cache_id label.
   *
   * @tparam meta_cache_id item label for static shape data
   * @param data matrix to be scattered column-wise and a dummy
   * argument on workers.
   * @return matrix chunk for a given worker.
   */
  template <int meta_cache_id>
  matrix_d scatter_matrix(const matrix_d& data) {
    typedef internal::mpi_parallel_call_cache<call_id, meta_cache_id,
                                              std::vector<size_type>>
        meta_cache;
    const std::vector<size_type>& dims
        = broadcast_array_1d_cached<meta_cache>({data.rows(), data.cols()});
    const size_type rows = dims[0];
    const size_type total_cols = dims[1];

    const std::vector<int> job_chunks = mpi_map_chunks(total_cols, 1);
    const std::vector<int> data_chunks = mpi_map_chunks(total_cols, rows);
    matrix_d local_data(rows, job_chunks[rank_]);
    if (rows * total_cols > 0) {
      if (rank_ == 0) {
        boost::mpi::scatterv(world_, data.data(), data_chunks,
                             local_data.data(), 0);
      } else {
        boost::mpi::scatterv(world_, local_data.data(), data_chunks[rank_], 0);
      }
    }

    return local_data;
  }

  void setup_call(const vector_d& shared_params, const matrix_d& job_params,
                  const std::vector<std::vector<double>>& x_r,
                  const std::vector<std::vector<int>>& x_i) {
    std::vector<int> job_chunks = mpi_map_chunks(job_params.cols(), 1);
    broadcast_array_1d_cached<cache_chunks>(job_chunks);

    local_shared_params_dbl_ = broadcast_vector<-1>(shared_params);
    local_job_params_dbl_ = scatter_matrix<-2>(job_params);

    // distribute const data if not yet cached
    scatter_array_2d_cached<cache_x_r>(x_r);
    scatter_array_2d_cached<cache_x_i>(x_i);
  }
};

template <int call_id, typename ReduceF, typename CombineF>
int mpi_parallel_call<call_id, ReduceF, CombineF>::num_outputs_per_job_ = -1;

}  // namespace math
}  // namespace stan

#endif

#endif
