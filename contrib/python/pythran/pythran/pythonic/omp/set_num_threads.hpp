#ifndef PYTHONIC_OMP_SET_NUM_THREADS_HPP
#define PYTHONIC_OMP_SET_NUM_THREADS_HPP

#include "pythonic/include/omp/set_num_threads.hpp"

#include <omp.h>
#include "pythonic/utils/functor.hpp"

PYTHONIC_NS_BEGIN

namespace omp
{
  void set_num_threads(long num_threads)
  {
    return omp_set_num_threads(num_threads);
  }
}
PYTHONIC_NS_END

#endif
