#ifndef STAN_LANG_AST_FUN_HAS_VAR_VIS_HPP
#define STAN_LANG_AST_FUN_HAS_VAR_VIS_HPP

#include <stan/lang/ast/variable_map.hpp>
#include <boost/variant/static_visitor.hpp>

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

    /**
     * Visitor to detect if an expression contains a non-data
     * variable.
     */
    struct has_var_vis : public boost::static_visitor<bool> {
      /**
       * Construct a non-data variable detection visitor.
       * @param[in] var_map global variable declaration mapping
       */
      explicit has_var_vis(const variable_map& var_map);

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const nil& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const int_literal& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const double_literal& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const array_expr& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const matrix_expr& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const row_vector_expr& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const variable& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const integrate_1d& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const integrate_ode& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const integrate_ode_control& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const algebra_solver& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const algebra_solver_control& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const map_rect& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const fun& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const index_op& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const index_op_sliced& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const conditional_op& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const binary_op& e) const;

      /**
       * Return true if the specified expression contains a non-data
       * variable.
       *
       * @param e expression
       * @return true if expression contains a non-data variable
       */
      bool operator()(const unary_op& e) const;

      /**
       * Reference to the global variable declaration mapping.
       */
      const variable_map& var_map_;
    };

  }
}
#endif
