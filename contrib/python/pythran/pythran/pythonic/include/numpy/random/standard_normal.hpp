#ifndef PYTHONIC_INCLUDE_NUMPY_RANDOM_STANDARD_NORMAL_HPP
#define PYTHONIC_INCLUDE_NUMPY_RANDOM_STANDARD_NORMAL_HPP

#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/types/ndarray.hpp"
#include "pythonic/include/types/NoneType.hpp"
#include "pythonic/include/types/tuple.hpp"

PYTHONIC_NS_BEGIN
namespace numpy
{
  namespace random
  {
    template <class pS>
    types::ndarray<double, pS> standard_normal(pS const &shape);

    auto standard_normal(long size)
        -> decltype(standard_normal(types::array<long, 1>{{size}}));

    double standard_normal(types::none_type d = {});

    DEFINE_FUNCTOR(pythonic::numpy::random, standard_normal);
  }
}
PYTHONIC_NS_END

#endif
