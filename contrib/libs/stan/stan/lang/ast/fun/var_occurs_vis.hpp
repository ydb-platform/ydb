#ifndef STAN_LANG_AST_FUN_VAR_OCCURS_VIS_HPP
#define STAN_LANG_AST_FUN_VAR_OCCURS_VIS_HPP

#include <boost/variant/static_visitor.hpp>
#include <string>

namespace stan {
  namespace lang {

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

    struct var_occurs_vis : public boost::static_visitor<bool> {
      /**
       * Construct a visitor to detect whether the specified variable
       * occurs in a statement.
       *
       * @param e variable to detect
       */
      explicit var_occurs_vis(const variable& e);

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return false
       */
      bool operator()(const nil& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return false
       */
      bool operator()(const int_literal& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return false
       */
      bool operator()(const double_literal& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if the variable occurs in any of the array
       * expression elements
       */
      bool operator()(const array_expr& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if the variable occurs in any of the matrix
       * expression elements
       */
      bool operator()(const matrix_expr& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if the variable occurs in any of the row_vector
       * expression elements
       */
      bool operator()(const row_vector_expr& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if variable is equal to the specifed variable
       */
      bool operator()(const variable& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if the variable occurs in the arguments
       */
      bool operator()(const fun& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if the variable occurs in the arguments
       */
      bool operator()(const integrate_1d& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if the variable occurs in the arguments
       */
      bool operator()(const integrate_ode& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if the variable occurs in the arguments
       */
      bool operator()(const integrate_ode_control& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if the variable occurs in the arguments
       */
      bool operator()(const algebra_solver& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if the variable occurs in the arguments
       */
      bool operator()(const algebra_solver_control& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if the variable occurs in the arguments
       */
      bool operator()(const map_rect& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if the variable occurs in the variable being
       * indexed or in any of the indexes
       */
      bool operator()(const index_op& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if the variable occurs in the variable being
       * indexed or in any of the indexes
       */
      bool operator()(const index_op_sliced& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if the variable occurs in the conditional or
       * result expressions
       */
      bool operator()(const conditional_op& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if the variable occurs in either of the operands
       */
      bool operator()(const binary_op& e) const;

      /**
       * Return true if the variable occurs in the specified
       * expression.
       *
       * @param[in] e expression
       * @return true if the variable occurs in the operand
       */
      bool operator()(const unary_op& e) const;

      /**
       * The name of the variable for which to search.
       */
      const std::string var_name_;
    };

  }
}
#endif
