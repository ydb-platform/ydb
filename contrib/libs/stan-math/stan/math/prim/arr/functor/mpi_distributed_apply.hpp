#ifdef STAN_MPI

#ifndef STAN_MATH_PRIM_ARR_FUNCTOR_MPI_DISTRIBUTED_APPLY_HPP
#define STAN_MATH_PRIM_ARR_FUNCTOR_MPI_DISTRIBUTED_APPLY_HPP

#include <stan/math/prim/arr/functor/mpi_command.hpp>

#include <boost/serialization/nvp.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/base_object.hpp>

namespace stan {
namespace math {

/**
 * MPI command template which calls the static method
 * distributed_apply of the given class F.
 *
 * Distributed MPI commands should be registered using the macro
 * STAN_REGISTER_MPI_DISTRIBUTED_APPLY called in the root
 * namespace.
 *
 * @tparam F type of functor with static distributed_apply method.
 */
template <typename F>
struct mpi_distributed_apply : public mpi_command {
  // declarations needed for boost.serialization (see
  // https://www.boost.org/doc/libs/1_66_0/libs/serialization/doc/index.html)
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive& ar, const unsigned int version) {
    ar& BOOST_SERIALIZATION_BASE_OBJECT_NVP(mpi_command);
  }

  /**
   * Executes the static member distributed_apply of the given
   * functor F.
   */
  void run() const { F::distributed_apply(); }
};

}  // namespace math
}  // namespace stan

#define STAN_REGISTER_MPI_DISTRIBUTED_APPLY(APPLY_FUNCTOR) \
  STAN_REGISTER_MPI_COMMAND(stan::math::mpi_distributed_apply<APPLY_FUNCTOR>)

#endif

#endif
