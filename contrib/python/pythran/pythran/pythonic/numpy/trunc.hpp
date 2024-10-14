#ifndef PYTHONIC_NUMPY_TRUNC_HPP
#define PYTHONIC_NUMPY_TRUNC_HPP

#include "pythonic/include/numpy/trunc.hpp"

#include "pythonic/utils/functor.hpp"
#include "pythonic/types/ndarray.hpp"
#include "pythonic/utils/numpy_traits.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{
#define NUMPY_NARY_FUNC_NAME trunc
#define NUMPY_NARY_FUNC_SYM xsimd::trunc
#include "pythonic/types/numpy_nary_expr.hpp"
}
PYTHONIC_NS_END

#endif
