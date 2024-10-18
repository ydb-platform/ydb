#ifndef PYTHONIC_INCLUDE_NUMPY_STD_HPP
#define PYTHONIC_INCLUDE_NUMPY_STD_HPP

#include "pythonic/include/numpy/var.hpp"
#include "pythonic/include/numpy/sqrt.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{

  template <class... Args>
  auto std_(Args &&... args)
      -> decltype(functor::sqrt{}(var(std::forward<Args>(args)...)));

  DEFINE_FUNCTOR(pythonic::numpy, std_);
}
PYTHONIC_NS_END

#endif
