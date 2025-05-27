#ifndef PYTHONIC_NUMPY_VECTORIZE_HPP
#define PYTHONIC_NUMPY_VECTORIZE_HPP

#include "pythonic/include/numpy/vectorize.hpp"
#include "pythonic/types/numpy_expr.hpp"

#include "pythonic/utils/functor.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{
  template <typename F>
  template <typename... T>
  auto vectorized<F>::operator()(T &&...args) const ->
      typename std::enable_if<!types::valid_numexpr_parameters<
                                  typename std::decay<T>::type...>::value,
                              decltype(F{}(std::forward<T>(args)...))>::type
  {
    return F{}(std::forward<T>(args)...);
  }

  template <class F>
  template <class... E>
  typename std::enable_if<
      types::valid_numexpr_parameters<typename std::decay<E>::type...>::value,
      types::numpy_expr<F, typename types::adapt_type<E, E...>::type...>>::type
  vectorized<F>::operator()(E &&...args) const
  {
    return {std::forward<E>(args)...};
  }

  template <class F>
  vectorized<F> vectorize(F const &)
  {
    return {};
  }

} // namespace numpy
PYTHONIC_NS_END

#endif
