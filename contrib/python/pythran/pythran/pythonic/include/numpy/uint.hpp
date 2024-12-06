#ifndef PYTHONIC_INCLUDE_NUMPY_UINT_HPP
#define PYTHONIC_INCLUDE_NUMPY_UINT_HPP

#include "pythonic/include/types/numpy_op_helper.hpp"
#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/utils/meta.hpp"
#include "pythonic/include/utils/numpy_traits.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

  namespace details
  {
    unsigned long uint();
    template <class V>
    unsigned long uint(V v);
  } // namespace details

#define NUMPY_NARY_FUNC_NAME uint
#define NUMPY_NARY_FUNC_SYM details::uint
#define NUMPY_NARY_EXTRA_METHOD using type = unsigned long;
#include "pythonic/include/types/numpy_nary_expr.hpp"
} // namespace numpy
PYTHONIC_NS_END

#endif
