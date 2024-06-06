#ifndef PYTHONIC_UTILS_NESTED_CONTAINER_HPP
#define PYTHONIC_UTILS_NESTED_CONTAINER_HPP

#include "pythonic/include/utils/nested_container.hpp"

#include <limits>
#include "pythonic/types/traits.hpp"
#include "pythonic/utils/numpy_traits.hpp"

PYTHONIC_NS_BEGIN
namespace utils
{

  template <class T>
  long nested_container_size<T>::flat_size(T const &t)
  {
    auto n = t.size();
    return n ? n * nested_container_size<typename std::conditional<
               // If we have a scalar or a complex, we want to stop
               // recursion, and then dispatch to bool specialization
               types::is_dtype<typename Type::value_type>::value, bool,
               typename Type::value_type>::type>::flat_size(*t.begin()) : 0;
  }

  /* Recursion stops on bool */
  template <class F>
  constexpr long nested_container_size<bool>::flat_size(F)
  {
    return 1;
  }
}
PYTHONIC_NS_END

#endif
