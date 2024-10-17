#ifndef PYTHONIC_INCLUDE_NUMPY_ARCCOSH_HPP
#define PYTHONIC_INCLUDE_NUMPY_ARCCOSH_HPP

#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/types/ndarray.hpp"
#include "pythonic/include/utils/numpy_traits.hpp"

#include <xsimd/xsimd.hpp>

PYTHONIC_NS_BEGIN

namespace numpy
{
#define NUMPY_NARY_FUNC_NAME arccosh
#define NUMPY_NARY_FUNC_SYM xsimd::acosh
#include "pythonic/include/types/numpy_nary_expr.hpp"
}
PYTHONIC_NS_END

#endif
