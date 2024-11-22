#ifndef PYTHONIC_INCLUDE_NUMPY_FLOAT128_HPP
#define PYTHONIC_INCLUDE_NUMPY_FLOAT128_HPP

#include "pythonic/include/types/numpy_op_helper.hpp"
#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/utils/numpy_traits.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{
  namespace details
  {

    long double float128();
    template <class V>
    long double float128(V v);
  } // namespace details

#define NUMPY_NARY_FUNC_NAME float128
#define NUMPY_NARY_FUNC_SYM details::float128
#define NUMPY_NARY_EXTRA_METHOD using type = long double;
#include "pythonic/include/types/numpy_nary_expr.hpp"
} // namespace numpy
PYTHONIC_NS_END

#endif
