#ifndef STAN_LANG_AST_FUN_HAS_NON_PARAM_VAR_VIS_HPP
#define STAN_LANG_AST_FUN_HAS_NON_PARAM_VAR_VIS_HPP

#include <stan/lang/ast/variable_map.hpp>
#include <boost/variant/static_visitor.hpp>

namespace stan {
  namespace lang {

    /**
     * Visitor to determine if an expression contains a variable that
     * is not declared as a parameter.
     */
    struct has_non_param_var_vis : public boost::static_visitor<bool> {
      /**
       * Construct the visitor with the specified global variable
       * declaration mapping.  This class will hold a reference to the
       * specified variable map, but will not alter it.
       *
       * @param[in] var_map
       */
      explicit has_non_param_var_vis(const variable_map& var_map);

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const nil& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const int_literal& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const double_literal& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const array_expr& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const matrix_expr& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const row_vector_expr& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const variable& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const integrate_1d& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const integrate_ode& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const integrate_ode_control& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
       bool operator()(const algebra_solver& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
       bool operator()(const algebra_solver_control& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
       bool operator()(const map_rect& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const fun& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const index_op& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const index_op_sliced& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const conditional_op& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const binary_op& e) const;

      /**
       * Return true if the specified expression contains a variable
       * not declared as a parameter.
       *
       * @param[in] e expression
       * @return true if contains a variable not declared as a parameter
       */
      bool operator()(const unary_op& e) const;

      /**
       * Reference to global variable declaration map.
       */
      const variable_map& var_map_;
    };

  }
}
#endif
