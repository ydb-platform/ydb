#ifndef PYTHONIC_NUMPY_NOTEQUAL_HPP
#define PYTHONIC_NUMPY_NOTEQUAL_HPP

#include "pythonic/include/numpy/not_equal.hpp"

#include "pythonic/utils/functor.hpp"
#include "pythonic/types/ndarray.hpp"
#include "pythonic/types/numpy_broadcast.hpp"
#include "pythonic/utils/numpy_traits.hpp"
#include "pythonic/operator_/ne.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{
#define NUMPY_NARY_FUNC_NAME not_equal
#define NUMPY_NARY_FUNC_SYM pythonic::operator_::ne
#include "pythonic/types/numpy_nary_expr.hpp"
}
PYTHONIC_NS_END

#endif
