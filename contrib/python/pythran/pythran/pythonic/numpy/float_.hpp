#ifndef PYTHONIC_NUMPY_FLOAT_HPP
#define PYTHONIC_NUMPY_FLOAT_HPP

#include "pythonic/include/numpy/float64.hpp"
#include "pythonic/include/numpy/float_.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

#define NUMPY_NARY_FUNC_NAME float_
#define NUMPY_NARY_FUNC_SYM details::float64
#include "pythonic/types/numpy_nary_expr.hpp"
} // namespace numpy
PYTHONIC_NS_END

#endif
