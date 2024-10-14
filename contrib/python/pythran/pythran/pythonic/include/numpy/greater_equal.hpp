#ifndef PYTHONIC_INCLUDE_NUMPY_GREATEREQUAL_HPP
#define PYTHONIC_INCLUDE_NUMPY_GREATEREQUAL_HPP

#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/types/ndarray.hpp"
#include "pythonic/include/operator_/ge.hpp"
#include "pythonic/include/types/numpy_broadcast.hpp"
#include "pythonic/include/utils/numpy_traits.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

#define NUMPY_NARY_FUNC_NAME greater_equal
#define NUMPY_NARY_FUNC_SYM pythonic::operator_::ge
#include "pythonic/include/types/numpy_nary_expr.hpp"
}
PYTHONIC_NS_END

#endif
