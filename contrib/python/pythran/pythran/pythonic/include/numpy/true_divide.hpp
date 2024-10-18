#ifndef PYTHONIC_INCLUDE_NUMPY_TRUEDIVIDE_HPP
#define PYTHONIC_INCLUDE_NUMPY_TRUEDIVIDE_HPP

#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/types/ndarray.hpp"
#include "pythonic/include/types/numpy_broadcast.hpp"
#include "pythonic/include/utils/numpy_traits.hpp"
#include "pythonic/include/operator_/div.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

// FIXME: this is ! always a true_divide...
#define NUMPY_NARY_FUNC_NAME true_divide
#define NUMPY_NARY_FUNC_SYM pythonic::operator_::div
#include "pythonic/include/types/numpy_nary_expr.hpp"
}
PYTHONIC_NS_END

#endif
