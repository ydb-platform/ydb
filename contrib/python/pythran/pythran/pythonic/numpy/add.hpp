#ifndef PYTHONIC_NUMPY_ADD_HPP
#define PYTHONIC_NUMPY_ADD_HPP

#include "pythonic/include/numpy/add.hpp"

#include "pythonic/utils/functor.hpp"
#include "pythonic/types/ndarray.hpp"
#include "pythonic/types/numpy_broadcast.hpp"
#include "pythonic/utils/numpy_traits.hpp"
#include "pythonic/operator_/add.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

#define NUMPY_NARY_FUNC_NAME add
#define NUMPY_NARY_FUNC_SYM pythonic::operator_::add
#include "pythonic/types/numpy_nary_expr.hpp"
}
PYTHONIC_NS_END

#endif
