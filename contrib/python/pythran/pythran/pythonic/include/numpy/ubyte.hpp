#ifndef PYTHONIC_INCLUDE_NUMPY_UBYTE_HPP
#define PYTHONIC_INCLUDE_NUMPY_UBYTE_HPP

#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/utils/meta.hpp"
#include "pythonic/include/utils/numpy_traits.hpp"
#include "pythonic/include/types/numpy_op_helper.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

  namespace details
  {

    unsigned char ubyte();
    template <class V>
    unsigned char ubyte(V v);
  }

#define NUMPY_NARY_FUNC_NAME ubyte
#define NUMPY_NARY_FUNC_SYM details::ubyte
#define NUMPY_NARY_EXTRA_METHOD using type = unsigned char;
#include "pythonic/include/types/numpy_nary_expr.hpp"
}
PYTHONIC_NS_END

#endif
