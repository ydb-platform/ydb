#ifndef PYTHONIC_INCLUDE_NUMPY_PROD_HPP
#define PYTHONIC_INCLUDE_NUMPY_PROD_HPP

#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/numpy/reduce.hpp"
#include "pythonic/include/operator_/imul.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

  template <class... Args>
  auto prod(Args &&... args) -> decltype(
      reduce<operator_::functor::imul>(std::forward<Args>(args)...));

  DEFINE_FUNCTOR(pythonic::numpy, prod);
}
PYTHONIC_NS_END

#endif
