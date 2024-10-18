#ifndef PYTHONIC_INCLUDE_NUMPY_BITWISE_OR_HPP
#define PYTHONIC_INCLUDE_NUMPY_BITWISE_OR_HPP

#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/types/ndarray.hpp"
#include "pythonic/include/operator_/or_.hpp"
#include "pythonic/include/utils/numpy_traits.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

#define NUMPY_NARY_FUNC_NAME bitwise_or
#define NUMPY_NARY_FUNC_SYM pythonic::operator_::or_
#include "pythonic/include/types/numpy_nary_expr.hpp"
}
PYTHONIC_NS_END

#endif
