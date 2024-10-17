#ifndef PYTHONIC_OPERATOR_CONTAINS_HPP
#define PYTHONIC_OPERATOR_CONTAINS_HPP

#include "pythonic/include/operator_/contains.hpp"

#include "pythonic/utils/functor.hpp"
#include "pythonic/builtins/in.hpp"

PYTHONIC_NS_BEGIN

namespace operator_
{
  template <class A, class B>
  auto contains(A &&a, B &&b)
      -> decltype(in(std::forward<A>(a), std::forward<B>(b)))
  {
    return in(std::forward<A>(a), std::forward<B>(b));
  }
}
PYTHONIC_NS_END

#endif
