#ifndef PYTHONIC_NUMPY_LOG10_HPP
#define PYTHONIC_NUMPY_LOG10_HPP

#include "pythonic/include/numpy/log10.hpp"

#include "pythonic/utils/functor.hpp"
#include "pythonic/types/ndarray.hpp"
#include "pythonic/utils/numpy_traits.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{
#define NUMPY_NARY_FUNC_NAME log10
#define NUMPY_NARY_FUNC_SYM xsimd::log10
#include "pythonic/types/numpy_nary_expr.hpp"
}
PYTHONIC_NS_END

#endif
