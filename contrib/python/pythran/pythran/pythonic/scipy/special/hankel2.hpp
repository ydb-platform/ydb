#ifndef PYTHONIC_SCIPY_SPECIAL_HANKEL2_HPP
#define PYTHONIC_SCIPY_SPECIAL_HANKEL2_HPP

#include "pythonic/include/scipy/special/hankel2.hpp"

#include "pythonic/types/ndarray.hpp"
#include "pythonic/types/complex.hpp"
#include "pythonic/utils/functor.hpp"
#include "pythonic/utils/numpy_traits.hpp"

#include "pythonic/utils/boost_local_config.hpp"
#include <boost/math/special_functions/hankel.hpp>

PYTHONIC_NS_BEGIN

namespace scipy
{
  namespace special
  {
    namespace details
    {
      template <class T0, class T1>
      std::complex<double> hankel2(T0 x, T1 y)
      {
        return boost::math::cyl_hankel_2(x, y);
      }
    }

#define NUMPY_NARY_FUNC_NAME hankel2
#define NUMPY_NARY_FUNC_SYM details::hankel2
#include "pythonic/types/numpy_nary_expr.hpp"
  }
}
PYTHONIC_NS_END

#endif
