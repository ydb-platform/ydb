#ifndef PYTHONIC_INCLUDE_NUMPY_INT8_HPP
#define PYTHONIC_INCLUDE_NUMPY_INT8_HPP

#include "pythonic/include/types/numpy_op_helper.hpp"
#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/utils/meta.hpp"
#include "pythonic/include/utils/numpy_traits.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

  namespace details
  {

    int8_t int8();
    template <class V>
    int8_t int8(V v);
  } // namespace details

#define NUMPY_NARY_FUNC_NAME int8
#define NUMPY_NARY_FUNC_SYM details::int8
#define NUMPY_NARY_EXTRA_METHOD using type = int8_t;
#include "pythonic/include/types/numpy_nary_expr.hpp"
} // namespace numpy
PYTHONIC_NS_END

#endif
