#ifdef STAN_MPI

#ifndef STAN_MATH_PRIM_ARR_FUNCTOR_MPI_COMMAND_HPP
#define STAN_MATH_PRIM_ARR_FUNCTOR_MPI_COMMAND_HPP

#include <boost/serialization/serialization.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/export.hpp>

namespace stan {
namespace math {

/**
 * A MPI command object is used to execute code on worker nodes.
 * The base class for all MPI commands defines the virtual run
 * method. All MPI commands must inherit from this class and
 * implement the run method which defines what will be executed on
 * each remote.
 *
 * <p>Note that a concrete command must also register itself with
 * the serialization boost library. A convenience macro
 * STAN_REGISTER_MPI_COMMAND is provided which does enable
 * additional tuning options for optimized MPI communication
 * performance (marking object as trivially serializable and
 * disable version tracking).
 *
 * For each declared command a call to the
 * STAN_REGISTER_MPI_COMMAND is required. This macro must be
 * called in the root namespace.
 */
struct mpi_command {
  // declarations needed for boost.serialization (see
  // https://www.boost.org/doc/libs/1_66_0/libs/serialization/doc/index.html)
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive& ar, const unsigned int version) {}

  virtual void run() const = 0;
};

}  // namespace math
}  // namespace stan

BOOST_SERIALIZATION_ASSUME_ABSTRACT(stan::math::mpi_command)

#define STAN_REGISTER_MPI_COMMAND(command)                              \
  BOOST_CLASS_IMPLEMENTATION(command,                                   \
                             boost::serialization::object_serializable) \
  BOOST_CLASS_EXPORT(command)                                           \
  BOOST_CLASS_TRACKING(command, boost::serialization::track_never)

#endif

#endif
