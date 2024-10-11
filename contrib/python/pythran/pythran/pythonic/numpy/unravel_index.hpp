#ifndef PYTHONIC_NUMPY_UNRAVEL_INDEX_HPP
#define PYTHONIC_NUMPY_UNRAVEL_INDEX_HPP

#include "pythonic/include/numpy/unravel_index.hpp"
#include "pythonic/builtins/ValueError.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{
  namespace
  {
    template <class E, class ShapeIt, class RetIt>
    void _unravel_index(E expr, ShapeIt shape_it, ShapeIt end_it, RetIt ret_it)
    {
      while (shape_it != end_it) {
        auto &v = *shape_it;
        auto tmp = expr / v;
        *ret_it = expr - v *tmp;
        expr = tmp;
        ++shape_it;
        ++ret_it;
      }
    }
  }

  template <class E, class S>
  typename std::enable_if<std::is_scalar<E>::value,
                          types::array<long, std::tuple_size<S>::value>>::type
  unravel_index(E const &expr, S const &shape, types::str const &order)
  {
    types::array<long, std::tuple_size<S>::value> ret;
    if (order[0] == "C") {
      _unravel_index(expr, shape.rbegin(), shape.rend(), ret.rbegin());
    } else if (order[0] == "F") {
      _unravel_index(expr, shape.begin(), shape.end(), ret.begin());
    } else {
      throw types::ValueError("Invalid order");
    }
    return ret;
  }
}
PYTHONIC_NS_END

#endif
