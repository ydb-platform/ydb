#ifndef STAN_MATH_PRIM_MAT_VECTORIZE_APPLY_SCALAR_UNARY_HPP
#define STAN_MATH_PRIM_MAT_VECTORIZE_APPLY_SCALAR_UNARY_HPP

#include <Eigen/Dense>
#include <vector>

namespace stan {
namespace math {

/**
 * Base template class for vectorization of unary scalar functions
 * defined by a template class <code>F</code> to a scalar,
 * standard library vector, or Eigen dense matrix expression
 * template.
 *
 * <p>The base class applies to any Eigen dense matrix expression
 * template.  Specializations define applications to scalars
 * (primitive or autodiff in the corresponding autodiff library
 * directories) or to standard library vectors of vectorizable
 * types (primitives, Eigen dense matrix expressions, or further
 * standard vectors).
 *
 * <p>Each specialization must define the typedef
 * <code>return_t</code> for the vectorized return type and the
 * function <code>apply</code> which defines the vectorization or
 * base application of the function defined statically by the
 * class F.  The function definition class F defines a static
 * function <code>fun()</code>, which defines the function's
 * behavior on scalars.
 *
 * @tparam F Type of function to apply.
 * @tparam T Type of argument to which function is applied.
 */
template <typename F, typename T>
struct apply_scalar_unary {
  /**
   * Type of underlying scalar for the matrix type T.
   */
  typedef typename Eigen::internal::traits<T>::Scalar scalar_t;

  /**
   * Return type for applying the function elementwise to a matrix
   * expression template of type T.
   */
  typedef Eigen::Matrix<scalar_t, T::RowsAtCompileTime, T::ColsAtCompileTime>
      return_t;

  /**
   * Return the result of applying the function defined by the
   * template parameter F to the specified matrix argument.
   *
   * @param x Matrix to which operation is applied.
   * @return Componentwise application of the function specified
   * by F to the specified matrix.
   */
  static inline return_t apply(const T& x) {
    return x.unaryExpr(
        [](scalar_t x) { return apply_scalar_unary<F, scalar_t>::apply(x); });
  }
};

/**
 * Template specialization for vectorized functions applying to
 * double arguments.
 *
 * @tparam F Type of function defining static apply function.
 */
template <typename F>
struct apply_scalar_unary<F, double> {
  /**
   * The return type, double.
   */
  typedef double return_t;

  /**
   * Apply the function specified by F to the specified argument.
   * This is defined through a direct application of
   * <code>F::fun()</code>, which must be defined for double
   * arguments.
   *
   * @param x Argument scalar.
   * @return Result of applying F to the scalar.
   */
  static inline return_t apply(double x) { return F::fun(x); }
};

/**
 * Template specialization for vectorized functions applying to
 * integer arguments.  Although the argument is integer, the
 * return type is specified as double.  This allows promotion of
 * integers to doubles in vectorized functions, or in containers.
 *
 * @tparam F Type of function defining static apply function.
 */
template <typename F>
struct apply_scalar_unary<F, int> {
  /**
   * The return type, double.
   */
  typedef double return_t;

  /**
   * Apply the function specified by F to the specified argument.
   * This is defined through a direct application of
   * <code>F::fun()</code>, which must be defined for double
   * arguments.
   *
   * @param x Argument scalar.
   * @return Result of applying F to the scalar.
   */
  static inline return_t apply(int x) { return F::fun(static_cast<double>(x)); }
};

/**
 * Template specialization for vectorized functions applying to
 * standard vector containers.  The lowest-level scalar type of
 * the argument will determine the return type.  Integers are
 * promoted to double values.
 *
 * @tparam F Class defining a static apply function.
 * @tparam T Type of element contained in standard vector.
 */
template <typename F, typename T>
struct apply_scalar_unary<F, std::vector<T> > {
  /**
   * Return type, which is calculated recursively as a standard
   * vector of the return type of the contained type T.
   */
  typedef typename std::vector<typename apply_scalar_unary<F, T>::return_t>
      return_t;

  /**
   * Apply the function specified by F elementwise to the
   * specified argument.  This is defined recursively through this
   * class applied to elements of type T.
   *
   * @param x Argument container.
   * @return Elementwise application of F to the elements of the
   * container.
   */
  static inline return_t apply(const std::vector<T>& x) {
    return_t fx(x.size());
    for (size_t i = 0; i < x.size(); ++i)
      fx[i] = apply_scalar_unary<F, T>::apply(x[i]);
    return fx;
  }
};

}  // namespace math
}  // namespace stan
#endif
