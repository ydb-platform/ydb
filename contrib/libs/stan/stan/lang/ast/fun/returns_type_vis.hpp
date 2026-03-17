#ifndef STAN_LANG_AST_FUN_RETURNS_TYPE_VIS_HPP
#define STAN_LANG_AST_FUN_RETURNS_TYPE_VIS_HPP

#include <stan/lang/ast/type/bare_expr_type.hpp>
#include <boost/variant/static_visitor.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    struct nil;
    struct assgn;
    struct sample;
    struct increment_log_prob_statement;
    struct expression;
    struct statements;
    struct for_statement;
    struct for_array_statement;
    struct for_matrix_statement;
    struct conditional_statement;
    struct while_statement;
    struct break_continue_statement;
    struct print_statement;
    struct reject_statement;
    struct no_op_statement;
    struct return_statement;

    /**
     * Visitor for reporting an error message if the statement is not
     * a return statement with a type specified at construction time.
     */
    struct returns_type_vis : public boost::static_visitor<bool> {
      /**
       * Construct visitor expecting a return statement with an
       * expression of the specified type, writing error messages to
       * the specified stream.
       *
       * @param[in] return_type type of return expression
       * @param[in, out] error_msgs stream to which error messages are
       * written 
       */
      returns_type_vis(const bare_expr_type& return_type,
                       std::ostream& error_msgs);

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return false
       */
      bool operator()(const nil& st) const;

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return false
       */
      bool operator()(const assgn& st) const;

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return false
       */
      bool operator()(const sample& st) const;

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return false
       */
      bool operator()(const increment_log_prob_statement& st) const;

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return false
       */
      bool operator()(const expression& st) const;

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return false
       */
      bool operator()(const statements& st) const;

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return false
       */
      bool operator()(const for_statement& st) const;

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return false
       */
      bool operator()(const for_array_statement& st) const;

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return false
       */
      bool operator()(const for_matrix_statement& st) const;

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return false
       */
      bool operator()(const conditional_statement& st) const;

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return false
       */
      bool operator()(const while_statement& st) const;

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return false
       */
      bool operator()(const break_continue_statement& st) const;

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return false
       */
      bool operator()(const print_statement& st) const;

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return false
       */
      bool operator()(const reject_statement& st) const;

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return false
       */
      bool operator()(const no_op_statement& st) const;

      /**
       * Return true if the specified statement is a return statement
       * with an expression of the type specified at construction
       * time.
       *
       * @param[in] st statement
       * @return true if the specifieid return type returns an
       * expression of the type specified at construction time
       */
      bool operator()(const return_statement& st) const;

      /**
       * The type of the returned expression expected.
       */
      bare_expr_type return_type_;

      /**
       * Stream to which error messages are written.
       */
      std::ostream& error_msgs_;
    };

  }
}
#endif
