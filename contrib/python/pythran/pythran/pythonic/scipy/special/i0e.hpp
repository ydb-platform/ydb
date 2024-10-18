#ifndef PYTHONIC_SCIPY_SPECIAL_I0_HPP
#define PYTHONIC_SCIPY_SPECIAL_I0_HPP

#include "pythonic/include/scipy/special/i0e.hpp"
#include "pythonic/scipy/special/chbevl.hpp"

#include "pythonic/types/ndarray.hpp"
#include "pythonic/utils/functor.hpp"
#include "pythonic/utils/numpy_traits.hpp"

PYTHONIC_NS_BEGIN

namespace scipy
{
  namespace special
  {
    namespace details
    {
      template <class T>
      double i0e(T x_)
      {
        double y;
        double x = x_;
        if (x < 0)
          x = -x;
        if (x <= 8.0) {
          y = (x / 2.0) - 2.0;
          return (chbevl(y, A));
        }

        return (chbevl(32.0 / x - 2.0, B) / sqrt(x));
      }
    }

#define NUMPY_NARY_FUNC_NAME i0e
#define NUMPY_NARY_FUNC_SYM details::i0e
#include "pythonic/types/numpy_nary_expr.hpp"
  }
}
PYTHONIC_NS_END

#endif
