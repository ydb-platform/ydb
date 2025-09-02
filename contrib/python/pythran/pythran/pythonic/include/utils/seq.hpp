#ifndef PYTHONIC_INCLUDE_UTILS_SEQ_HPP
#define PYTHONIC_INCLUDE_UTILS_SEQ_HPP

PYTHONIC_NS_BEGIN

namespace utils
{

  // make_integer_sequence<N>() = integer_sequence<0, ..., N-1>

  template <class T, T...>
  struct integer_sequence {
  };

  template <std::size_t... S>
  using index_sequence = integer_sequence<std::size_t, S...>;

  namespace details
  {
    template <class Left, class Right>
    struct make_integer_sequence_join;

    template <class T, T... Left, T... Right>
    struct make_integer_sequence_join<integer_sequence<T, Left...>,
                                      integer_sequence<T, Right...>> {
      using type = integer_sequence<T, Left..., (sizeof...(Left) + Right)...>;
    };

    template <class T, std::size_t N, T... S>
    struct make_integer_sequence
        : make_integer_sequence_join<
              typename make_integer_sequence<T, N / 2>::type,
              typename make_integer_sequence<T, N - N / 2>::type> {
    };
    template <class T>
    struct make_integer_sequence<T, 0> {
      using type = integer_sequence<T>;
    };
    template <class T>
    struct make_integer_sequence<T, 1> {
      using type = integer_sequence<T, 0>;
    };
  } // namespace details

  template <class T, std::size_t N>
  using make_integer_sequence =
      typename details::make_integer_sequence<T, N>::type;
  template <std::size_t N>
  using make_index_sequence =
      typename details::make_integer_sequence<std::size_t, N>::type;

  // make_reversed_integer_sequence<T, N>() = integer_sequence<T, N-1, ..., 0>

  namespace details
  {

    template <class T, std::size_t N, T... S>
    struct make_reversed_integer_sequence
        : make_reversed_integer_sequence<T, N - 1, sizeof...(S), S...> {
    };

    template <class T, T... S>
    struct make_reversed_integer_sequence<T, 0, S...> {
      using type = integer_sequence<T, S...>;
    };
  } // namespace details

  template <class T, std::size_t N>
  using make_reversed_integer_sequence =
      typename details::make_reversed_integer_sequence<T, N>::type;
  template <std::size_t N>
  using make_reversed_index_sequence =
      typename details::make_reversed_integer_sequence<std::size_t, N>::type;

  // make_repeated_type<A, 3>() => type_sequence<A, A, A>
  template <class... Tys>
  struct type_sequence {
  };

  namespace details
  {
    template <class T, std::size_t N, class... Tys>
    struct repeated_type : repeated_type<T, N - 1, T, Tys...> {
    };

    template <class T, class... Tys>
    struct repeated_type<T, 0, Tys...> {
      using type = type_sequence<Tys...>;
    };
  } // namespace details
  template <class T, std::size_t N>
  struct repeated_type : details::repeated_type<T, N> {
  };

  template <class T, std::size_t N>
  using make_repeated_type = typename repeated_type<T, N>::type;
} // namespace utils
PYTHONIC_NS_END

#endif
