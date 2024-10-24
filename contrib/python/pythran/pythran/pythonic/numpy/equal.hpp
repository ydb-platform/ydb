#ifndef PYTHONIC_NUMPY_EQUAL_HPP
#define PYTHONIC_NUMPY_EQUAL_HPP

#include "pythonic/include/numpy/equal.hpp"

#include "pythonic/utils/functor.hpp"
#include "pythonic/types/ndarray.hpp"
#include "pythonic/types/numpy_broadcast.hpp"
#include "pythonic/utils/numpy_traits.hpp"
#include "pythonic/operator_/eq.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

#define NUMPY_NARY_FUNC_NAME equal
#define NUMPY_NARY_FUNC_SYM pythonic::operator_::eq
#include "pythonic/types/numpy_nary_expr.hpp"
}
PYTHONIC_NS_END

#endif
