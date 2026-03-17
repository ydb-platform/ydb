#ifndef STAN_LANG_AST_NODE_EXPRESSION_HPP
#define STAN_LANG_AST_NODE_EXPRESSION_HPP

#include <boost/variant/recursive_variant.hpp>
#include <ostream>
#include <string>
#include <vector>
#include <cstddef>

namespace stan {
  namespace lang {

    struct bare_expr_type;
    struct nil;
    struct int_literal;
    struct double_literal;
    struct array_expr;
    struct matrix_expr;
    struct row_vector_expr;
    struct variable;
    struct fun;
    struct integrate_1d;
    struct integrate_ode;
    struct integrate_ode_control;
    struct algebra_solver;
    struct algebra_solver_control;
    struct map_rect;
    struct index_op;
    struct index_op_sliced;
    struct conditional_op;
    struct binary_op;
    struct unary_op;

    struct expression {
      typedef boost::variant<boost::recursive_wrapper<nil>,
                             boost::recursive_wrapper<int_literal>,
                             boost::recursive_wrapper<double_literal>,
                             boost::recursive_wrapper<array_expr>,
                             boost::recursive_wrapper<matrix_expr>,
                             boost::recursive_wrapper<row_vector_expr>,
                             boost::recursive_wrapper<variable>,
                             boost::recursive_wrapper<fun>,
                             boost::recursive_wrapper<integrate_1d>,
                             boost::recursive_wrapper<integrate_ode>,
                             boost::recursive_wrapper<integrate_ode_control>,
                             boost::recursive_wrapper<algebra_solver>,
                             boost::recursive_wrapper<algebra_solver_control>,
                             boost::recursive_wrapper<map_rect>,
                             boost::recursive_wrapper<index_op>,
                             boost::recursive_wrapper<index_op_sliced>,
                             boost::recursive_wrapper<conditional_op>,
                             boost::recursive_wrapper<binary_op>,
                             boost::recursive_wrapper<unary_op> >
      expression_t;

      expression();
      expression(const expression& e);

      expression(const nil& expr);  // NOLINT(runtime/explicit)
      expression(const int_literal& expr);  // NOLINT(runtime/explicit)
      expression(const double_literal& expr);  // NOLINT(runtime/explicit)
      expression(const array_expr& expr);  // NOLINT(runtime/explicit)
      expression(const matrix_expr& expr);  // NOLINT(runtime/explicit)
      expression(const row_vector_expr& expr);  // NOLINT(runtime/explicit)
      expression(const variable& expr);  // NOLINT(runtime/explicit)
      expression(const fun& expr);  // NOLINT(runtime/explicit)
      expression(const integrate_1d& expr);  // NOLINT(runtime/explicit)
      expression(const integrate_ode& expr);  // NOLINT(runtime/explicit)
      expression(const integrate_ode_control& expr);  // NOLINT
      expression(const algebra_solver& expr);  // NOLINT(runtime/explicit)
      expression(const algebra_solver_control& expr);  // NOLINT
      expression(const map_rect& expr);  // NOLINT
      expression(const index_op& expr);  // NOLINT(runtime/explicit)
      expression(const index_op_sliced& expr);  // NOLINT(runtime/explicit)
      expression(const conditional_op& expr);  // NOLINT(runtime/explicit)
      expression(const binary_op& expr);  // NOLINT(runtime/explicit)
      expression(const unary_op& expr);  // NOLINT(runtime/explicit)
      expression(const expression_t& expr_);  // NOLINT(runtime/explicit)

      bare_expr_type bare_type() const;
      int total_dims() const;
      std::string to_string() const;

      expression& operator+=(const expression& rhs);
      expression& operator-=(const expression& rhs);
      expression& operator*=(const expression& rhs);
      expression& operator/=(const expression& rhs);

      expression_t expr_;
    };

  }
}
#endif
