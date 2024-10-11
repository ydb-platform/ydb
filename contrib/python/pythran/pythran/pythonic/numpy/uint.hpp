#ifndef PYTHONIC_NUMPY_UINT_HPP
#define PYTHONIC_NUMPY_UINT_HPP

#include "pythonic/include/numpy/uint.hpp"

#include "pythonic/types/numpy_op_helper.hpp"
#include "pythonic/utils/functor.hpp"
#include "pythonic/utils/meta.hpp"
#include "pythonic/utils/numpy_traits.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

  namespace details
  {

    inline unsigned long uint()
    {
      return {};
    }

    template <class V>
    unsigned long uint(V v)
    {
      return v;
    }
  } // namespace details

#define NUMPY_NARY_FUNC_NAME uint
#define NUMPY_NARY_FUNC_SYM details::uint
#include "pythonic/types/numpy_nary_expr.hpp"
} // namespace numpy
PYTHONIC_NS_END

#endif
