#ifdef STAN_MPI

#ifndef STAN_MATH_PRIM_ARR_FUNCTOR_MPI_CLUSTER_HPP
#define STAN_MATH_PRIM_ARR_FUNCTOR_MPI_CLUSTER_HPP

#include <stan/math/prim/arr/functor/mpi_command.hpp>

#error #include <boost/mpi/allocator.hpp>
#error #include <boost/mpi/collectives.hpp>
#error #include <boost/mpi/communicator.hpp>
#error #include <boost/mpi/datatype.hpp>
#error #include <boost/mpi/environment.hpp>
#error #include <boost/mpi/nonblocking.hpp>
#error #include <boost/mpi/operations.hpp>

#include <boost/serialization/access.hpp>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/shared_ptr.hpp>

#include <mutex>
#include <vector>
#include <memory>

namespace stan {
namespace math {

/**
 * Exception used to stop workers nodes from further listening to
 * commands send from the root.
 */
class mpi_stop_listen : public std::exception {
  virtual const char* what() const throw() {
    return "Stopping MPI listening mode.";
  }
};

/**
 * Exception thrown whenever the MPI resource is busy
 */
class mpi_is_in_use : public std::exception {
  virtual const char* what() const throw() { return "MPI resource is in use."; }
};

/**
 * MPI command used to stop childs nodes from listening for
 * further commands.
 */
struct mpi_stop_worker : public mpi_command {
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive& ar, const unsigned int version) {
    ar& BOOST_SERIALIZATION_BASE_OBJECT_NVP(mpi_command);
  }
  void run() const {
    boost::mpi::communicator world;
    throw mpi_stop_listen();
  }
};

/**
 * Maps jobs of given chunk size to workers and returning a vector
 * of counts. The returned vector is indexed by the rank of each
 * worker and has size equal to the # of workers. Each count per
 * worker is the product of the number of assigned jobs times the
 * chunk size. The jobs are deterministically assigned to
 * workers. This is used for static scheduling of jobs internally.
 *
 * So with num_workers workers, then the counts for worker with
 * given rank is
 *
 * chunks[rank] = floor(num_jobs/num_workers) * chunk_size
 *
 * The remainder jobs num_jobs % num_workers are assigned to rank
 * >=1 workers such that the root (rank = 0) has a little less
 * assigned chunks unless num_jobs < num_workes in which case the
 * first num_jobs nodes recieve a job (including the root).
 *
 * @param num_jobs Total number of jobs to dispatch
 * @param chunk_size Chunk size per job
 * @return vector indexed by rank with the total number of
 * elements mapped to a given worker
 */
inline std::vector<int> mpi_map_chunks(std::size_t num_jobs,
                                       std::size_t chunk_size = 1) {
  boost::mpi::communicator world;
  const std::size_t world_size = world.size();

  std::vector<int> chunks(world_size, num_jobs / world_size);

  const std::size_t delta_r = chunks[0] == 0 ? 0 : 1;

  for (std::size_t r = 0; r != num_jobs % world_size; ++r)
    ++chunks[r + delta_r];

  for (std::size_t i = 0; i != world_size; ++i)
    chunks[i] *= chunk_size;

  return chunks;
}

template <typename T>
std::unique_lock<std::mutex> mpi_broadcast_command();

/**
 * MPI cluster holds MPI resources and must be initialized only
 * once in any MPI program. The RAII principle is used and MPI
 * resource allocation and deallocation is done when constructing
 * and destructing this object. This must occur only once in any
 * program as the MPI layer will fail otherwise.
 *
 * The available MPI resources are determined from environment
 * variables which are usually set by the mpirun command which is
 * recommended to launch any MPI program.
 *
 * The mpirun program automatically starts the number of
 * pre-specified processes. The boost mpi library is used to
 * interface with MPI. In boost mpi terminology we refer to the
 * overall MPI ressources as world. Each process has an assigned
 * rank which is a numeric index running from 0 to the number of
 * processes-1 (also called the world size). We refer to the rank
 * = 0 process as the root process and all others (rank > 0) are
 * workers.
 *
 */
struct mpi_cluster {
  boost::mpi::environment env;
  boost::mpi::communicator world_;
  std::size_t const rank_ = world_.rank();

  mpi_cluster() {}

  ~mpi_cluster() {
    // the destructor will ensure that the childs are being
    // shutdown
    stop_listen();
  }

  /**
   * Switches cluster into listening mode. That is, for the root
   * process we only flag that the workers are now listening and
   * as such can recieve commands while for the non-root processes
   * we enter into a listening state. In the listening state on
   * the non-root processes we wait for broadcasts of mpi_command
   * objects which are initiated on the root using the
   * mpi_broadcast_command function below. Each recieved
   * mpi_command is executed using the virtual run method.
   */
  void listen() {
    listening_status() = true;
    if (rank_ == 0) {
      return;
    }

    try {
      // lock on the workers the cluster as MPI commands must be
      // initiated from the root and any attempt to do this on the
      // workers must fail
      std::unique_lock<std::mutex> worker_lock(in_use());
      while (1) {
        std::shared_ptr<mpi_command> work;

        boost::mpi::broadcast(world_, work, 0);

        work->run();
      }
    } catch (const mpi_stop_listen& e) {
    }
  }

  /**
   * Stops listening state of the cluster. This happens
   * automatically upon destruction. It is achieved by sending the
   * mpi_stop_worker command.
   */
  void stop_listen() {
    if (rank_ == 0 && listening_status()) {
      mpi_broadcast_command<mpi_stop_worker>();
    }
    listening_status() = false;
  }

  /**
   * Returns the current listening state of the cluster.
   */
  static bool& listening_status() {
    static bool listening_status = false;
    return listening_status;
  }

  /**
   * Returns a reference to the global in use mutex
   */
  static std::mutex& in_use() {
    static std::mutex in_use_mutex;
    return in_use_mutex;
  }
};

/**
 * Broadcasts a command instance to the listening cluster. This
 * function must be called on the root whenever the cluster is in
 * listening mode and errs otherwise.
 *
 * @param command shared pointer to an instance of a command class
 * derived from mpi_command
 * @return A unique_lock instance locking the mpi_cluster
 */
inline std::unique_lock<std::mutex> mpi_broadcast_command(
    std::shared_ptr<mpi_command>& command) {
  boost::mpi::communicator world;

  if (world.rank() != 0)
    throw std::runtime_error("only root may broadcast commands.");

  if (!mpi_cluster::listening_status())
    throw std::runtime_error("cluster is not listening to commands.");

  std::unique_lock<std::mutex> cluster_lock(mpi_cluster::in_use(),
                                            std::try_to_lock);

  if (!cluster_lock.owns_lock())
    throw mpi_is_in_use();

  boost::mpi::broadcast(world, command, 0);

  return cluster_lock;
}

/**
 * Broadcasts default constructible commands to the cluster.
 *
 * @tparam T default constructible command class derived from
 * mpi_command
 * @return A unique_lock instance locking the mpi_cluster
 */
template <typename T>
std::unique_lock<std::mutex> mpi_broadcast_command() {
  std::shared_ptr<mpi_command> command(new T);

  return mpi_broadcast_command(command);
}

}  // namespace math
}  // namespace stan

#endif

#endif
