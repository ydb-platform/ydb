#ifndef PYTHONIC_INCLUDE_NUMPY_INTC_HPP
#define PYTHONIC_INCLUDE_NUMPY_INTC_HPP

#include "pythonic/include/types/numpy_op_helper.hpp"
#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/utils/meta.hpp"
#include "pythonic/include/utils/numpy_traits.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

  namespace details
  {

    int intc();
    template <class V>
    int intc(V v);
  } // namespace details

#define NUMPY_NARY_FUNC_NAME intc
#define NUMPY_NARY_FUNC_SYM details::intc
#define NUMPY_NARY_EXTRA_METHOD using type = int;
#include "pythonic/include/types/numpy_nary_expr.hpp"
} // namespace numpy
PYTHONIC_NS_END

#endif
