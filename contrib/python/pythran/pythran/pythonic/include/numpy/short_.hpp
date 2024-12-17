#ifndef PYTHONIC_INCLUDE_NUMPY_SHORT__HPP
#define PYTHONIC_INCLUDE_NUMPY_SHORT__HPP

#include "pythonic/include/types/numpy_op_helper.hpp"
#include "pythonic/include/utils/functor.hpp"
#include "pythonic/include/utils/meta.hpp"
#include "pythonic/include/utils/numpy_traits.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

  namespace details
  {

    short short_();
    template <class V>
    short short_(V v);
  } // namespace details

#define NUMPY_NARY_FUNC_NAME short_
#define NUMPY_NARY_FUNC_SYM details::short_
#define NUMPY_NARY_EXTRA_METHOD using type = short;
#include "pythonic/include/types/numpy_nary_expr.hpp"
} // namespace numpy
PYTHONIC_NS_END

#endif
