#ifndef STAN_MATH_REV_MAT_FUNCTOR_ADJ_JAC_APPLY_HPP
#define STAN_MATH_REV_MAT_FUNCTOR_ADJ_JAC_APPLY_HPP

#include <stan/math/prim/scal/meta/scalar_type.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/value_of.hpp>
#include <stan/math/rev/scal/fun/value_of.hpp>
#include <stan/math/rev/scal/meta/is_var.hpp>
#include <limits>
#include <tuple>
#include <vector>

namespace stan {
namespace math {

namespace internal {
/**
 * Invoke the functor f with arguments given in t and indexed in the index
 * sequence I
 *
 * @tparam F Type of functor
 * @tparam Tuple Type of tuple containing arguments
 * @tparam I Index sequence going from 0 to std::tuple_size<T>::value - 1
 * inclusive
 * @param f functor callable
 * @param t tuple of arguments
 * @param i placeholder variable for index sequence
 */
template <class F, class Tuple, std::size_t... I>
constexpr auto apply_impl(const F& f, const Tuple& t,
                          std::index_sequence<I...> i) {
  return f(std::get<I>(t)...);
}

/**
 * Call the functor f with the tuple of arguments t, like:
 *
 * f(std::get<0>(t), std::get<1>(t), ...)
 *
 * TODO: replace this with implementation in C++ std when C++17 is available
 *
 * @tparam F Type of functor
 * @tparam Tuple Type of tuple containing arguments
 * @param f functor callable
 * @param t tuple of arguments
 */
template <class F, class Tuple>
constexpr auto apply(const F& f, const Tuple& t) {
  return apply_impl(f, t, std::make_index_sequence<std::tuple_size<Tuple>{}>{});
}

/**
 * Store the adjoint in y_vi[0] in y_adj
 *
 * @tparam size dimensionality of M
 * @param[in] y_vi pointer to pointer to vari
 * @param[in] M
 * @param[out] y_adj reference to variable where adjoint is to be stored
 */
template <size_t size>
void build_y_adj(vari** y_vi, const std::array<int, size>& M, double& y_adj) {
  y_adj = y_vi[0]->adj_;
}

/**
 * Store the adjoints from y_vi in y_adj
 *
 * @tparam size dimensionality of M
 * @param[in] y_vi pointer to pointers to varis
 * @param[in] M shape of y_adj
 * @param[out] y_adj reference to std::vector where adjoints are to be stored
 */
template <size_t size>
void build_y_adj(vari** y_vi, const std::array<int, size>& M,
                 std::vector<double>& y_adj) {
  y_adj.resize(M[0]);
  for (size_t m = 0; m < y_adj.size(); ++m)
    y_adj[m] = y_vi[m]->adj_;
}

/**
 * Store the adjoints from y_vi in y_adj
 *
 * @tparam size dimensionality of M
 * @param[in] y_vi pointer to pointers to varis
 * @param[in] M shape of y_adj
 * @param[out] y_adj reference to Eigen::Matrix where adjoints are to be stored
 */
template <size_t size, int R, int C>
void build_y_adj(vari** y_vi, const std::array<int, size>& M,
                 Eigen::Matrix<double, R, C>& y_adj) {
  y_adj.resize(M[0], M[1]);
  for (int m = 0; m < y_adj.size(); ++m)
    y_adj(m) = y_vi[m]->adj_;
}

/**
 * Compute the dimensionality of the given template argument. The
 * definition of dimensionality is deferred to specializations. By
 * default don't have a value (fail to compile)
 */
template <typename T>
struct compute_dims {};

/**
 * Compute the dimensionality of the given template argument. Double
 * types hav dimensionality zero.
 */
template <>
struct compute_dims<double> {
  static constexpr size_t value = 0;
};

/**
 * Compute the dimensionality of the given template argument.
 * std::vector has dimension 1
 */
template <typename T>
struct compute_dims<std::vector<T>> {
  static constexpr size_t value = 1;
};

/**
 * compute the dimensionality of the given template argument.
 * Eigen::Matrix types all have dimension two
 */
template <typename T, int R, int C>
struct compute_dims<Eigen::Matrix<T, R, C>> {
  static constexpr size_t value = 2;
};
}  // namespace internal

/**
 * adj_jac_vari interfaces a user supplied functor  with the reverse mode
 * autodiff. It allows someone to implement functions with custom reverse mode
 * autodiff without having to deal with autodiff types.
 *
 * The requirements on the functor F are described in the documentation for
 * adj_jac_apply
 *
 * Targs (the input argument types) can be any mix of double, var, or
 * Eigen::Matrices with double or var scalar components
 *
 * @tparam F class of functor
 * @tparam Targs Types of arguments
 */
template <typename F, typename... Targs>
struct adj_jac_vari : public vari {
  std::array<bool, sizeof...(Targs)> is_var_;
  using FReturnType
      = std::result_of_t<F(decltype(is_var_), decltype(value_of(Targs()))...)>;

  F f_;
  std::array<int, sizeof...(Targs)> offsets_;
  vari** x_vis_;
  std::array<int, internal::compute_dims<FReturnType>::value> M_;
  vari** y_vi_;

  /**
   * count_memory returns count (the first argument) + the number of varis used
   * in the second argument + the number of arguments used to encode the
   * variadic tail args.
   *
   * The adj_jac_vari constructor uses this to figure out how much space to
   * allocate in x_vis_.
   *
   * The array offsets_ is populated with values to indicate where in x_vis_ the
   * vari pointers for each argument will be stored.
   *
   * Each of the arguments can be an Eigen::Matrix with var or double scalar
   * types, a std::vector with var, double, or int scalar types, or a var, a
   * double, or an int.
   *
   * @tparam R Eigen Matrix row type
   * @tparam C Eigen Matrix column type
   * @tparam Pargs Types of rest of arguments
   * @param count rolling count of number of varis that must be allocated
   * @param x next argument to have its varis counted
   * @param args the rest of the arguments (that will be iterated through
   * recursively)
   */
  template <int R, int C, typename... Pargs>
  size_t count_memory(size_t count, const Eigen::Matrix<var, R, C>& x,
                      const Pargs&... args) {
    static constexpr int t = sizeof...(Targs) - sizeof...(Pargs) - 1;
    offsets_[t] = count;
    count += x.size();
    return count_memory(count, args...);
  }

  template <int R, int C, typename... Pargs>
  size_t count_memory(size_t count, const Eigen::Matrix<double, R, C>& x,
                      const Pargs&... args) {
    static constexpr int t = sizeof...(Targs) - sizeof...(Pargs) - 1;
    offsets_[t] = count;
    return count_memory(count, args...);
  }

  template <typename... Pargs>
  size_t count_memory(size_t count, const std::vector<var>& x,
                      const Pargs&... args) {
    static constexpr int t = sizeof...(Targs) - sizeof...(Pargs) - 1;
    offsets_[t] = count;
    count += x.size();
    return count_memory(count, args...);
  }

  template <typename... Pargs>
  size_t count_memory(size_t count, const std::vector<double>& x,
                      const Pargs&... args) {
    static constexpr int t = sizeof...(Targs) - sizeof...(Pargs) - 1;
    offsets_[t] = count;
    return count_memory(count, args...);
  }

  template <typename... Pargs>
  size_t count_memory(size_t count, const std::vector<int>& x,
                      const Pargs&... args) {
    static constexpr int t = sizeof...(Targs) - sizeof...(Pargs) - 1;
    offsets_[t] = count;
    return count_memory(count, args...);
  }

  template <typename... Pargs>
  size_t count_memory(size_t count, const var& x, const Pargs&... args) {
    static constexpr int t = sizeof...(Targs) - sizeof...(Pargs) - 1;
    offsets_[t] = count;
    count += 1;
    return count_memory(count, args...);
  }

  template <typename... Pargs>
  size_t count_memory(size_t count, const double& x, const Pargs&... args) {
    static constexpr int t = sizeof...(Targs) - sizeof...(Pargs) - 1;
    offsets_[t] = count;
    return count_memory(count, args...);
  }

  template <typename... Pargs>
  size_t count_memory(size_t count, const int& x, const Pargs&... args) {
    static constexpr int t = sizeof...(Targs) - sizeof...(Pargs) - 1;
    offsets_[t] = count;
    return count_memory(count, args...);
  }

  size_t count_memory(size_t count) { return count; }

  /**
   * prepare_x_vis populates x_vis_ with the varis from each of its
   * input arguments. The vari pointers for argument n are copied into x_vis_ at
   * the index starting at offsets_[n]. For Eigen::Matrix types, this copying is
   * done in with column major ordering.
   *
   * Each of the arguments can be an Eigen::Matrix with var or double scalar
   * types, a std::vector with var, double, or int scalar types, or a var, a
   * double, or an int.
   *
   * @tparam R Eigen Matrix row type
   * @tparam C Eigen Matrix column type
   * @tparam Pargs Types of the rest of the arguments to be processed
   * @param x next argument to have its vari pointers copied if necessary
   * @param args the rest of the arguments (that will be iterated through
   * recursively)
   */
  template <int R, int C, typename... Pargs>
  void prepare_x_vis(const Eigen::Matrix<var, R, C>& x, const Pargs&... args) {
    static constexpr int t = sizeof...(Targs) - sizeof...(Pargs) - 1;
    for (int i = 0; i < x.size(); ++i) {
      x_vis_[offsets_[t] + i] = x(i).vi_;
    }
    prepare_x_vis(args...);
  }

  template <int R, int C, typename... Pargs>
  void prepare_x_vis(const Eigen::Matrix<double, R, C>& x,
                     const Pargs&... args) {
    prepare_x_vis(args...);
  }

  template <typename... Pargs>
  void prepare_x_vis(const std::vector<var>& x, const Pargs&... args) {
    static constexpr int t = sizeof...(Targs) - sizeof...(Pargs) - 1;
    for (size_t i = 0; i < x.size(); ++i) {
      x_vis_[offsets_[t] + i] = x[i].vi_;
    }
    prepare_x_vis(args...);
  }

  template <typename... Pargs>
  void prepare_x_vis(const std::vector<double>& x, const Pargs&... args) {
    prepare_x_vis(args...);
  }

  template <typename... Pargs>
  void prepare_x_vis(const std::vector<int>& x, const Pargs&... args) {
    prepare_x_vis(args...);
  }

  template <typename... Pargs>
  void prepare_x_vis(const var& x, const Pargs&... args) {
    static constexpr int t = sizeof...(Targs) - sizeof...(Pargs) - 1;
    x_vis_[offsets_[t]] = x.vi_;
    prepare_x_vis(args...);
  }

  template <typename... Pargs>
  void prepare_x_vis(const double& x, const Pargs&... args) {
    prepare_x_vis(args...);
  }

  template <typename... Pargs>
  void prepare_x_vis(const int& x, const Pargs&... args) {
    prepare_x_vis(args...);
  }

  /**
   * Initializes is_var_ with true if the scalar type in each argument
   * is a var (and false if not)
   */
  adj_jac_vari()
      : vari(std::numeric_limits<double>::quiet_NaN()),  // The val_ in this
                                                         // vari is unused
        is_var_({{is_var<typename scalar_type<Targs>::type>::value...}}),
        x_vis_(NULL),
        y_vi_(NULL) {}

  /**
   * Return a var with a new vari holding the given value
   *
   * @param val_y output of F::operator()
   * @return var
   */
  var build_return_varis_and_vars(const double& val_y) {
    y_vi_ = ChainableStack::instance().memalloc_.alloc_array<vari*>(1);
    y_vi_[0] = new vari(val_y, false);

    return y_vi_[0];
  }

  /**
   * Return a std::vector of vars created from newly allocated varis initialized
   * with the values of val_y
   *
   * @param val_y output of F::operator()
   * @return std::vector of vars
   */
  std::vector<var> build_return_varis_and_vars(
      const std::vector<double>& val_y) {
    M_[0] = val_y.size();
    std::vector<var> var_y(M_[0]);

    y_vi_
        = ChainableStack::instance().memalloc_.alloc_array<vari*>(var_y.size());
    for (size_t m = 0; m < var_y.size(); ++m) {
      y_vi_[m] = new vari(val_y[m], false);
      var_y[m] = y_vi_[m];
    }

    return var_y;
  }

  /**
   * Return an Eigen::Matrix of vars created from newly allocated varis
   * initialized with the values of val_y. The shape of the new matrix comes
   * from M_
   *
   * @tparam R Eigen row type
   * @tparam C Eigen column type
   * @param val_y output of F::operator()
   * @return Eigen::Matrix of vars
   */
  template <int R, int C>
  Eigen::Matrix<var, R, C> build_return_varis_and_vars(
      const Eigen::Matrix<double, R, C>& val_y) {
    M_[0] = val_y.rows();
    M_[1] = val_y.cols();
    Eigen::Matrix<var, R, C> var_y(M_[0], M_[1]);

    y_vi_
        = ChainableStack::instance().memalloc_.alloc_array<vari*>(var_y.size());
    for (int m = 0; m < var_y.size(); ++m) {
      y_vi_[m] = new vari(val_y(m), false);
      var_y(m) = y_vi_[m];
    }

    return var_y;
  }

  void prepare_x_vis() {}

  /**
   * The adj_jac_vari functor
   *  1. Initializes an instance of the user defined functor F
   *  2. Calls operator() on the F instance with the double values from the
   * input args
   *  3. Saves copies of the varis pointed to by the input vars for subsequent
   * calls to chain
   *  4. Calls build_return_varis_and_vars to construct the appropriate output
   * data structure of vars
   *
   * Each of the arguments can be an Eigen::Matrix with var or double scalar
   * types, a std::vector with var, double, or int scalar types, or a var, a
   * double, or an int.
   *
   * @param args Input arguments
   * @return Output of f_ as vars
   */
  auto operator()(const Targs&... args) {
    x_vis_ = ChainableStack::instance().memalloc_.alloc_array<vari*>(
        count_memory(0, args...));

    prepare_x_vis(args...);

    return build_return_varis_and_vars(f_(is_var_, value_of(args)...));
  }

  /**
   * Accumulate, if necessary, the values of y_adj_jac into the
   * adjoints of the varis pointed to by the appropriate elements
   * of x_vis_. Recursively calls accumulate_adjoints on the rest of the
   * arguments.
   *
   * @tparam R Eigen Matrix row type
   * @tparam C Eigen Matrix column type
   * @tparam Pargs Types of the rest of adjoints to accumulate
   * @param y_adj_jac set of values to be accumulated in adjoints
   * @param args the rest of the arguments (that will be iterated through
   * recursively)
   */
  template <int R, int C, typename... Pargs>
  void accumulate_adjoints(const Eigen::Matrix<double, R, C>& y_adj_jac,
                           const Pargs&... args) {
    static constexpr int t = sizeof...(Targs) - sizeof...(Pargs) - 1;
    if (is_var_[t]) {
      for (int n = 0; n < y_adj_jac.size(); ++n) {
        x_vis_[offsets_[t] + n]->adj_ += y_adj_jac(n);
      }
    }

    accumulate_adjoints(args...);
  }

  /**
   * Accumulate, if necessary, the values of y_adj_jac into the
   * adjoints of the varis pointed to by the appropriate elements
   * of x_vis_. Recursively calls accumulate_adjoints on the rest of the
   * arguments.
   *
   * @tparam Pargs Types of the rest of adjoints to accumulate
   * @param y_adj_jac set of values to be accumulated in adjoints
   * @param args the rest of the arguments (that will be iterated through
   * recursively)
   */
  template <typename... Pargs>
  void accumulate_adjoints(const std::vector<double>& y_adj_jac,
                           const Pargs&... args) {
    static constexpr int t = sizeof...(Targs) - sizeof...(Pargs) - 1;
    if (is_var_[t]) {
      for (size_t n = 0; n < y_adj_jac.size(); ++n)
        x_vis_[offsets_[t] + n]->adj_ += y_adj_jac[n];
    }

    accumulate_adjoints(args...);
  }

  /**
   * Recursively call accumulate_adjoints with args. There are no adjoints to
   * accumulate for std::vector<int> arguments.
   *
   * @tparam Pargs Types of the rest of adjoints to accumulate
   * @param y_adj_jac ignored
   * @param args the rest of the arguments (that will be iterated through
   * recursively)
   */
  template <typename... Pargs>
  void accumulate_adjoints(const std::vector<int>& y_adj_jac,
                           const Pargs&... args) {
    accumulate_adjoints(args...);
  }

  /**
   * Accumulate, if necessary, the value of y_adj_jac into the
   * adjoint of the vari pointed to by the appropriate element
   * of x_vis_. Recursively calls accumulate_adjoints on the rest of the
   * arguments.
   *
   * @tparam Pargs Types of the rest of adjoints to accumulate
   * @param y_adj_jac next set of adjoints to be accumulated
   * @param args the rest of the arguments (that will be iterated through
   * recursively)
   */
  template <typename... Pargs>
  void accumulate_adjoints(const double& y_adj_jac, const Pargs&... args) {
    static constexpr int t = sizeof...(Targs) - sizeof...(Pargs) - 1;
    if (is_var_[t]) {
      x_vis_[offsets_[t]]->adj_ += y_adj_jac;
    }

    accumulate_adjoints(args...);
  }

  /**
   * Recursively call accumulate_adjoints with args. There are no adjoints to
   * accumulate for int arguments.
   *
   * @tparam Pargs Types of the rest of adjoints to accumulate
   * @param y_adj_jac ignored
   * @param args the rest of the arguments (that will be iterated through
   * recursively)
   */
  template <typename... Pargs>
  void accumulate_adjoints(const int& y_adj_jac, const Pargs&... args) {
    accumulate_adjoints(args...);
  }

  void accumulate_adjoints() {}

  /**
   * Propagate the adjoints at the output varis (y_vi_) back to the input
   * varis (x_vis_) by:
   * 1. packing the adjoints in an appropriate container using build_y_adj
   * 2. using the multiply_adjoint_jacobian function of the user defined functor
   * to compute what the adjoints on x_vis_ should be
   * 3. accumulating the adjoints into the varis pointed to by elements of
   * x_vis_ using accumulate_adjoints
   *
   * This operation may be called multiple times during the life of the vari
   */
  void chain() {
    FReturnType y_adj;

    internal::build_y_adj(y_vi_, M_, y_adj);
    auto y_adj_jacs = f_.multiply_adjoint_jacobian(is_var_, y_adj);

    internal::apply(
        [this](auto&&... args) { this->accumulate_adjoints(args...); },
        y_adj_jacs);
  }
};

/**
 * Return the result of applying the function defined by a nullary construction
 * of F to the specified input argument
 *
 * adj_jac_apply makes it possible to write efficient reverse-mode
 * autodiff code without ever touching Stan's autodiff internals
 *
 * Mathematically, to use a function in reverse mode autodiff, you need to be
 * able to evaluate the function (y = f(x)) and multiply the Jacobian of that
 * function (df(x)/dx) by a vector.
 *
 * As an example, pretend there exists some large, complicated function, L(x1,
 * x2), which contains our smaller function f(x1, x2). The goal of autodiff is
 * to compute the partials dL/dx1 and dL/dx2. If we break the large function
 * into pieces:
 *
 * y = f(x1, x2)
 * L = g(y)
 *
 * If we were given dL/dy we could compute dL/dx1 by the product dL/dy * dy/dx1
 * or dL/dx2 by the product dL/dy * dy/dx2
 *
 * Because y = f(x1, x2), dy/dx1 is just df(x1, x2)/dx1, the Jacobian of the
 * function we're trying to define with x2 held fixed. A similar thing happens
 * for dy/dx2. In vector form,
 *
 * dL/dx1 = (dL/dy)' * df(x1, x2)/dx1 and
 * dL/dx2 = (dL/dy)' * df(x1, x2)/dx2
 *
 * So implementing f(x1, x2) and the products above are all that is required
 * mathematically to implement reverse-mode autodiff for a function.
 *
 * adj_jac_apply takes as a template argument a functor F that supplies the
 * non-static member functions (leaving exact template arguments off):
 *
 * (required) Eigen::VectorXd operator()(const std::array<bool, size>&
 * needs_adj, const T1& x1..., const T2& x2, ...)
 *
 * where there can be any number of input arguments x1, x2, ... and T1, T2, ...
 * can be either doubles or any Eigen::Matrix type with double scalar values.
 * needs_adj is an array of size equal to the number of input arguments
 * indicating whether or not the adjoint^T Jacobian product must be computed for
 * each input argument. This argument is passed to operator() so that any
 * unnecessary preparatory calculations for multiply_adjoint_jacobian can be
 * avoided if possible.
 *
 * (required) std::tuple<T1, T2, ...> multiply_adjoint_jacobian(const
 * std::array<bool, size>& needs_adj, const Eigen::VectorXd& adj)
 *
 * where T1, T2, etc. are the same types as in operator(), needs_adj is the same
 * as in operator(), and adj is the vector dL/dy.
 *
 * operator() is responsible for computing f(x) and multiply_adjoint_jacobian is
 * responsible for computing the necessary adjoint transpose Jacobian products
 * (which frequently does not require the calculation of the full Jacobian).
 *
 * operator() will be called before multiply_adjoint_jacobian is called, and is
 * only called once in the lifetime of the functor multiply_adjoint_jacobian is
 * called after operator() and may be called multiple times for any single
 * functor
 *
 * The functor supplied to adj_jac_apply must be careful to allocate any
 * variables it defines in the autodiff arena because its destructor will
 * never be called and memory will leak if allocated anywhere else.
 *
 * Targs (the input argument types) can be any mix of doubles, vars, ints,
 * std::vectors with double, var, or int scalar components, or
 * Eigen::Matrix s of any shape with var or double scalar components
 *
 * @tparam F functor to be connected to the autodiff stack
 * @tparam Targs types of arguments to pass to functor
 * @param args input to the functor
 * @return the result of the specified operation wrapped up in vars
 */
template <typename F, typename... Targs>
auto adj_jac_apply(const Targs&... args) {
  auto vi = new adj_jac_vari<F, Targs...>();

  return (*vi)(args...);
}

}  // namespace math
}  // namespace stan
#endif
