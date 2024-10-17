#ifndef PYTHONIC_BUILTIN_ITER_HPP
#define PYTHONIC_BUILTIN_ITER_HPP

#include "pythonic/include/builtins/iter.hpp"

#include "pythonic/utils/functor.hpp"

PYTHONIC_NS_BEGIN

namespace builtins
{

  namespace details
  {
    /// details iter implementation

    template <class T>
    iter<T>::iter()
    {
    }

    // FIXME : There is a dangling reference as data.begin() is ! the one
    // from data "saved" in the "iter" struct
    template <class T>
    iter<T>::iter(T data)
        : iterator(data.begin()), _end(data.end()), data(data)
    {
    }

    template <class T>
    typename iter<T>::iterator &iter<T>::begin()
    {
      return *this;
    }

    template <class T>
    typename iter<T>::iterator const &iter<T>::begin() const
    {
      return *this;
    }

    template <class T>
    typename iter<T>::iterator const &iter<T>::end() const
    {
      return _end;
    }
  }

  /// iter implementation

  template <class T>
  details::iter<
      typename std::remove_cv<typename std::remove_reference<T>::type>::type>
  iter(T &&t)
  {
    return {std::forward<T>(t)};
  }
}
PYTHONIC_NS_END

#endif
