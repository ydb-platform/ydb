#ifndef STAN_MATH_REV_CORE_VAR_HPP
#define STAN_MATH_REV_CORE_VAR_HPP

#include <stan/math/rev/core/vari.hpp>
#include <stan/math/rev/core/grad.hpp>
#include <stan/math/rev/core/chainable_alloc.hpp>
#include <boost/math/tools/config.hpp>
#include <ostream>
#include <vector>
#include <complex>
#include <string>
#include <exception>

namespace stan {
namespace math {

// forward declare
static void grad(vari* vi);

/**
 * Independent (input) and dependent (output) variables for gradients.
 *
 * This class acts as a smart pointer, with resources managed by
 * an agenda-based memory manager scoped to a single gradient
 * calculation.
 *
 * An var is constructed with a double and used like any
 * other scalar.  Arithmetical functions like negation, addition,
 * and subtraction, as well as a range of mathematical functions
 * like exponentiation and powers are overridden to operate on
 * var values objects.
 */
class var {
 public:
  // FIXME: doc what this is for
  typedef double Scalar;

  /**
   * Pointer to the implementation of this variable.
   *
   * This value should not be modified, but may be accessed in
   * <code>var</code> operators to construct <code>vari</code>
   * instances.
   */
  vari* vi_;

  /**
   * Return <code>true</code> if this variable has been
   * declared, but not been defined.  Any attempt to use an
   * undefined variable's value or adjoint will result in a
   * segmentation fault.
   *
   * @return <code>true</code> if this variable does not yet have
   * a defined variable.
   */
  bool is_uninitialized() { return (vi_ == static_cast<vari*>(nullptr)); }

  /**
   * Construct a variable for later assignment.
   *
   * This is implemented as a no-op, leaving the underlying implementation
   * dangling.  Before an assignment, the behavior is thus undefined just
   * as for a basic double.
   */
  var() : vi_(static_cast<vari*>(0U)) {}

  /**
   * Construct a variable from a pointer to a variable implementation.
   *
   * @param vi Variable implementation.
   */
  var(vari* vi) : vi_(vi) {}  // NOLINT

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint.
   *
   * @param x Value of the variable.
   */
  var(float x) : vi_(new vari(static_cast<double>(x))) {}  // NOLINT

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument as
   * a value and a zero adjoint.
   *
   * @param x Value of the variable.
   */
  var(double x) : vi_(new vari(x)) {}  // NOLINT

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint.
   *
   * @param x Value of the variable.
   */
  var(long double x) : vi_(new vari(x)) {}  // NOLINT

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint.
   *
   * @param x Value of the variable.
   */
  var(bool x) : vi_(new vari(static_cast<double>(x))) {}  // NOLINT

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint.
   *
   * @param x Value of the variable.
   */
  var(char x) : vi_(new vari(static_cast<double>(x))) {}  // NOLINT

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint.
   *
   * @param x Value of the variable.
   */
  var(short x) : vi_(new vari(static_cast<double>(x))) {}  // NOLINT

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint.
   *
   * @param x Value of the variable.
   */
  var(int x) : vi_(new vari(static_cast<double>(x))) {}  // NOLINT

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint.
   *
   * @param x Value of the variable.
   */
  var(long x) : vi_(new vari(static_cast<double>(x))) {}  // NOLINT

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint.
   *
   * @param x Value of the variable.
   */
  var(unsigned char x)  // NOLINT(runtime/explicit)
      : vi_(new vari(static_cast<double>(x))) {}

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint.
   *
   * @param x Value of the variable.
   */
  // NOLINTNEXTLINE
  var(unsigned short x) : vi_(new vari(static_cast<double>(x))) {}

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint.
   *
   * @param x Value of the variable.
   */
  var(unsigned int x) : vi_(new vari(static_cast<double>(x))) {}  // NOLINT

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint.
   *
   * @param x Value of the variable.
   */
  // NOLINTNEXTLINE
  var(unsigned long x) : vi_(new vari(static_cast<double>(x))) {}  // NOLINT

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint. Only works
   * if the imaginary part is zero.
   *
   * @param x Value of the variable.
   */
  explicit var(const std::complex<double>& x) {
    if (imag(x) == 0) {
      vi_ = new vari(real(x));
    } else {
      std::stringstream ss;
      ss << "Imaginary part of std::complex used to construct var"
         << " must be zero. Found real part = " << real(x) << " and "
         << " found imaginary part = " << imag(x) << std::endl;
      std::string msg = ss.str();
      throw std::invalid_argument(msg);
    }
  }

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint. Only works
   * if the imaginary part is zero.
   *
   * @param x Value of the variable.
   */
  explicit var(const std::complex<float>& x) {
    if (imag(x) == 0) {
      vi_ = new vari(static_cast<double>(real(x)));
    } else {
      std::stringstream ss;
      ss << "Imaginary part of std::complex used to construct var"
         << " must be zero. Found real part = " << real(x) << " and "
         << " found imaginary part = " << imag(x) << std::endl;
      std::string msg = ss.str();
      throw std::invalid_argument(msg);
    }
  }

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint. Only works
   * if the imaginary part is zero.
   *
   * @param x Value of the variable.
   */
  explicit var(const std::complex<long double>& x) {
    if (imag(x) == 0) {
      vi_ = new vari(static_cast<double>(real(x)));
    } else {
      std::stringstream ss;
      ss << "Imaginary part of std::complex used to construct var"
         << " must be zero. Found real part = " << real(x) << " and "
         << " found imaginary part = " << imag(x) << std::endl;
      std::string msg = ss.str();
      throw std::invalid_argument(msg);
    }
  }

#ifdef _WIN64

  // these two ctors are for Win64 to enable 64-bit signed
  // and unsigned integers, because long and unsigned long
  // are still 32-bit

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint.
   *
   * @param x Value of the variable.
   */
  var(size_t x) : vi_(new vari(static_cast<double>(x))) {}  // NOLINT

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint.
   *
   * @param x Value of the variable.
   */
  var(ptrdiff_t x) : vi_(new vari(static_cast<double>(x))) {}  // NOLINT
#endif

#ifdef BOOST_MATH_USE_FLOAT128

  // this ctor is for later GCCs that have the __float128
  // type enabled, because it gets enabled by boost

  /**
   * Construct a variable from the specified arithmetic argument
   * by constructing a new <code>vari</code> with the argument
   * cast to <code>double</code>, and a zero adjoint.
   *
   * @param x Value of the variable.
   */
  var(__float128 x) : vi_(new vari(static_cast<double>(x))) {}  // NOLINT

#endif

  /**
   * Return the value of this variable.
   *
   * @return The value of this variable.
   */
  inline double val() const { return vi_->val_; }

  /**
   * Return the derivative of the root expression with
   * respect to this expression.  This method only works
   * after one of the <code>grad()</code> methods has been
   * called.
   *
   * @return Adjoint for this variable.
   */
  inline double adj() const { return vi_->adj_; }

  /**
   * Compute the gradient of this (dependent) variable with respect to
   * the specified vector of (independent) variables, assigning the
   * specified vector to the gradient.
   *
   * The grad() function does <i>not</i> recover memory.  In Stan
   * 2.4 and earlier, this function did recover memory.
   *
   * @param x Vector of independent variables.
   * @param g Gradient vector of partial derivatives of this
   * variable with respect to x.
   */
  void grad(std::vector<var>& x, std::vector<double>& g) {
    stan::math::grad(vi_);
    g.resize(x.size());
    for (size_t i = 0; i < x.size(); ++i)
      g[i] = x[i].vi_->adj_;
  }

  /**
   * Compute the gradient of this (dependent) variable with respect
   * to all (independent) variables.
   *
   * The grad() function does <i>not</i> recover memory.
   */
  void grad() { stan::math::grad(vi_); }

  // POINTER OVERRIDES

  /**
   * Return a reference to underlying implementation of this variable.
   *
   * If <code>x</code> is of type <code>var</code>, then applying
   * this operator, <code>*x</code>, has the same behavior as
   * <code>*(x.vi_)</code>.
   *
   * <i>Warning</i>:  The returned reference does not track changes to
   * this variable.
   *
   * @return variable
   */
  inline vari& operator*() { return *vi_; }

  /**
   * Return a pointer to the underlying implementation of this variable.
   *
   * If <code>x</code> is of type <code>var</code>, then applying
   * this operator, <code>x-&gt;</code>, behaves the same way as
   * <code>x.vi_-&gt;</code>.
   *
   * <i>Warning</i>: The returned result does not track changes to
   * this variable.
   */
  inline vari* operator->() { return vi_; }

  // COMPOUND ASSIGNMENT OPERATORS

  /**
   * The compound add/assignment operator for variables (C++).
   *
   * If this variable is a and the argument is the variable b,
   * then (a += b) behaves exactly the same way as (a = a + b),
   * creating an intermediate variable representing (a + b).
   *
   * @param b The variable to add to this variable.
   * @return The result of adding the specified variable to this variable.
   */
  inline var& operator+=(const var& b);

  /**
   * The compound add/assignment operator for scalars (C++).
   *
   * If this variable is a and the argument is the scalar b, then
   * (a += b) behaves exactly the same way as (a = a + b).  Note
   * that the result is an assignable lvalue.
   *
   * @param b The scalar to add to this variable.
   * @return The result of adding the specified variable to this variable.
   */
  inline var& operator+=(double b);

  /**
   * The compound subtract/assignment operator for variables (C++).
   *
   * If this variable is a and the argument is the variable b,
   * then (a -= b) behaves exactly the same way as (a = a - b).
   * Note that the result is an assignable lvalue.
   *
   * @param b The variable to subtract from this variable.
   * @return The result of subtracting the specified variable from
   * this variable.
   */
  inline var& operator-=(const var& b);

  /**
   * The compound subtract/assignment operator for scalars (C++).
   *
   * If this variable is a and the argument is the scalar b, then
   * (a -= b) behaves exactly the same way as (a = a - b).  Note
   * that the result is an assignable lvalue.
   *
   * @param b The scalar to subtract from this variable.
   * @return The result of subtracting the specified variable from this
   * variable.
   */
  inline var& operator-=(double b);

  /**
   * The compound multiply/assignment operator for variables (C++).
   *
   * If this variable is a and the argument is the variable b,
   * then (a *= b) behaves exactly the same way as (a = a * b).
   * Note that the result is an assignable lvalue.
   *
   * @param b The variable to multiply this variable by.
   * @return The result of multiplying this variable by the
   * specified variable.
   */
  inline var& operator*=(const var& b);

  /**
   * The compound multiply/assignment operator for scalars (C++).
   *
   * If this variable is a and the argument is the scalar b, then
   * (a *= b) behaves exactly the same way as (a = a * b).  Note
   * that the result is an assignable lvalue.
   *
   * @param b The scalar to multiply this variable by.
   * @return The result of multplying this variable by the specified
   * variable.
   */
  inline var& operator*=(double b);

  /**
   * The compound divide/assignment operator for variables (C++).  If this
   * variable is a and the argument is the variable b, then (a /= b)
   * behaves exactly the same way as (a = a / b).  Note that the
   * result is an assignable lvalue.
   *
   * @param b The variable to divide this variable by.
   * @return The result of dividing this variable by the
   * specified variable.
   */
  inline var& operator/=(const var& b);

  /**
   * The compound divide/assignment operator for scalars (C++).
   *
   * If this variable is a and the argument is the scalar b, then
   * (a /= b) behaves exactly the same way as (a = a / b).  Note
   * that the result is an assignable lvalue.
   *
   * @param b The scalar to divide this variable by.
   * @return The result of dividing this variable by the specified
   * variable.
   */
  inline var& operator/=(double b);

  /**
   * Write the value of this auto-dif variable and its adjoint to
   * the specified output stream.
   *
   * @param os Output stream to which to write.
   * @param v Variable to write.
   * @return Reference to the specified output stream.
   */
  friend std::ostream& operator<<(std::ostream& os, const var& v) {
    if (v.vi_ == nullptr)
      return os << "uninitialized";
    return os << v.val();
  }
};

}  // namespace math
}  // namespace stan
#endif
