#ifndef PYTHONIC_INCLUDE_OMP_IN_PARALLEL_HPP
#define PYTHONIC_INCLUDE_OMP_IN_PARALLEL_HPP

#include <omp.h>
#include "pythonic/include/utils/functor.hpp"

PYTHONIC_NS_BEGIN

namespace omp
{
  bool in_parallel();

  DEFINE_FUNCTOR(pythonic::omp, in_parallel);
}
PYTHONIC_NS_END

#endif
