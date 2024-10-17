#ifndef PYTHONIC_OPERATOR_DELITEM_HPP
#define PYTHONIC_OPERATOR_DELITEM_HPP

#include "pythonic/include/operator_/delitem.hpp"

#include "pythonic/utils/functor.hpp"
#include "pythonic/builtins/None.hpp"

PYTHONIC_NS_BEGIN

namespace operator_
{

  template <class A, class B>
  types::none_type delitem(A &&a, B &&b)
  {
    std::forward<A>(a).remove(std::forward<B>(b));
    return builtins::None;
  }
}
PYTHONIC_NS_END

#endif
