#ifndef PYTHONIC_INCLUDE_NUMPY_ARCSINH_HPP
#define PYTHONIC_INCLUDE_NUMPY_ARCSINH_HPP

#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/types/ndarray.hpp"
#include "pythonic/include/utils/numpy_traits.hpp"

#include <xsimd/xsimd.hpp>

PYTHONIC_NS_BEGIN

namespace numpy
{

#define NUMPY_NARY_FUNC_NAME arcsinh
#define NUMPY_NARY_FUNC_SYM xsimd::asinh
#include "pythonic/include/types/numpy_nary_expr.hpp"
}
PYTHONIC_NS_END

#endif
