#ifndef PYTHONIC_INCLUDE_NUMPY_VECTORIZE_HPP
#define PYTHONIC_INCLUDE_NUMPY_VECTORIZE_HPP

#include "pythonic/include/types/numpy_expr.hpp"
#include "pythonic/include/utils/functor.hpp"

PYTHONIC_NS_BEGIN

namespace numpy
{
  template <class F>
  struct vectorized {
    using callable = void;
    template <typename... T>
    auto operator()(T &&...args) const ->
        typename std::enable_if<!types::valid_numexpr_parameters<
                                    typename std::decay<T>::type...>::value,
                                decltype(F{}(std::forward<T>(args)...))>::type;

    template <class... E>
    typename std::enable_if<
        types::valid_numexpr_parameters<typename std::decay<E>::type...>::value,
        types::numpy_expr<F,
                          typename types::adapt_type<E, E...>::type...>>::type
    operator()(E &&...args) const;
  };

  template <class F>
  vectorized<F> vectorize(F const &);

  DEFINE_FUNCTOR(pythonic::numpy, vectorize);
} // namespace numpy
PYTHONIC_NS_END

#endif
