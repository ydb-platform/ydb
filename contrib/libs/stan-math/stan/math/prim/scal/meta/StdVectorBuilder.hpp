#ifndef STAN_MATH_PRIM_SCAL_META_STDVECTORBUILDER_HPP
#define STAN_MATH_PRIM_SCAL_META_STDVECTORBUILDER_HPP

#include <stan/math/prim/scal/meta/VectorBuilderHelper.hpp>
#include <stan/math/prim/scal/meta/contains_std_vector.hpp>

namespace stan {

/**
 *  StdVectorBuilder allocates type T1 values to be used as
 *  intermediate values.
 *
 *  If any of T2 to T7 have the type std::vector<T>, the data
 *  function will return a std::vector<T1> as output.
 *
 *  If none of T2 to T7 have type std::vector<T>, the data function
 *  will return a T1 as output.
 *
 *  Whichever case is true, StdVectorBuilder presents a consistent
 *  vector-like interface (through size() and operator[](int i))
 *  for constructing the output
 *
 *  The difference between this and VectorBuilder is that VectorBuilder
 *  counts Eigen::Matrix<T, Dynamic, 1> and Eigen::Matrix<T, 1, Dynamic>
 *  types as vectors.
 *
 *  @tparam used boolean variable indicating whether this instance
 *      is used. If this is false, there is no storage allocated
 *      and operator[] throws a std::logic_error.
 *  @tparam T1 Type of vector to build
 */
template <bool used, typename T1, typename T2, typename T3 = double,
          typename T4 = double, typename T5 = double, typename T6 = double,
          typename T7 = double>
class StdVectorBuilder {
 private:
  typedef VectorBuilderHelper<
      T1, used, contains_std_vector<T2, T3, T4, T5, T6, T7>::value>
      helper;

 public:
  typedef typename helper::type type;
  helper a;

  explicit StdVectorBuilder(size_t n) : a(n) {}

  T1& operator[](size_t i) { return a[i]; }

  inline type data() { return a.data(); }
};

}  // namespace stan
#endif
