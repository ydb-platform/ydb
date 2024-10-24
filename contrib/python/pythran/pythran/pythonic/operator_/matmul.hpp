#ifndef PYTHONIC_OPERATOR_MATMUL_HPP
#define PYTHONIC_OPERATOR_MATMUL_HPP

#include "pythonic/include/operator_/matmul.hpp"

#include "pythonic/utils/functor.hpp"
#include "pythonic/numpy/dot.hpp"

PYTHONIC_NS_BEGIN

namespace operator_
{

  template <class A, class B>
  auto matmul(A &&a, B &&b)
      -> decltype(numpy::functor::dot{}(std::forward<A>(a), std::forward<B>(b)))
  {
    return numpy::functor::dot{}(std::forward<A>(a), std::forward<B>(b));
  }
}
PYTHONIC_NS_END

#endif
