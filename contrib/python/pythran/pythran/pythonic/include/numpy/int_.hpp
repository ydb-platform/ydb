#ifndef PYTHONIC_INCLUDE_NUMPY_INT__HPP
#define PYTHONIC_INCLUDE_NUMPY_INT__HPP

#include "pythonic/include/types/numpy_op_helper.hpp"
#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/utils/meta.hpp"
#include "pythonic/include/utils/numpy_traits.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

  namespace details
  {
    long int_();
    template <class V>
    long int_(V v);
  } // namespace details

#define NUMPY_NARY_FUNC_NAME int_
#define NUMPY_NARY_FUNC_SYM details::int_
#define NUMPY_NARY_EXTRA_METHOD using type = long;
#include "pythonic/include/types/numpy_nary_expr.hpp"
} // namespace numpy
PYTHONIC_NS_END

#endif
