#ifndef PYTHONIC_INCLUDE_RANDOM_RANDRANGE_HPP
#define PYTHONIC_INCLUDE_RANDOM_RANDRANGE_HPP

#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/random/random.hpp"

#include <cmath>

PYTHONIC_NS_BEGIN

namespace random
{
  long randrange(long stop);
  long randrange(long start, long stop);
  long randrange(long start, long stop, long step);

  DEFINE_FUNCTOR(pythonic::random, randrange)
}
PYTHONIC_NS_END

#endif
