#ifndef PYTHONIC_INCLUDE_NUMPY_UINTC_HPP
#define PYTHONIC_INCLUDE_NUMPY_UINTC_HPP

#include "pythonic/include/types/numpy_op_helper.hpp"
#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/utils/meta.hpp"
#include "pythonic/include/utils/numpy_traits.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

  namespace details
  {

    unsigned uintc();
    template <class V>
    unsigned uintc(V v);
  } // namespace details

#define NUMPY_NARY_FUNC_NAME uintc
#define NUMPY_NARY_FUNC_SYM details::uintc
#define NUMPY_NARY_EXTRA_METHOD using type = unsigned;
#include "pythonic/include/types/numpy_nary_expr.hpp"
} // namespace numpy
PYTHONIC_NS_END

#endif
