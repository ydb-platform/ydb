#ifndef OPERATOR_NAME
#error OPERATOR_NAME ! defined
#endif

#ifndef OPERATOR_SYMBOL
#error OPERATOR_SYMBOL ! defined
#endif

#ifndef OPERATOR_ISYMBOL
#error OPERATOR_ISYMBOL ! defined
#endif
#include "pythonic/utils/functor.hpp"

#ifdef USE_XSIMD
#include <xsimd/xsimd.hpp>
#endif

PYTHONIC_NS_BEGIN

namespace operator_
{

  template <class A, class B>
  auto OPERATOR_NAME(bool, A &&a, B &&b, ...)
      -> decltype(std::forward<A>(a) OPERATOR_SYMBOL std::forward<B>(b))
  {
    return std::forward<A>(a) OPERATOR_SYMBOL std::forward<B>(b);
  }
  template <class A, class B>
  auto OPERATOR_NAME(bool, A &&a, B &&b, std::nullptr_t)
      -> decltype(std::forward<A>(a) OPERATOR_ISYMBOL std::forward<B>(b))
  {
    return std::forward<A>(a) OPERATOR_ISYMBOL std::forward<B>(b);
  }
}
PYTHONIC_NS_END
#undef OPERATOR_NAME
#undef OPERATOR_SYMBOL
#undef OPERATOR_ISYMBOL
