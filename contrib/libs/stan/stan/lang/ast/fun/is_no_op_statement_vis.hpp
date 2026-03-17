#ifndef STAN_LANG_AST_FUN_IS_NO_OP_STATEMENT_VIS_HPP
#define STAN_LANG_AST_FUN_IS_NO_OP_STATEMENT_VIS_HPP

#include <boost/variant/static_visitor.hpp>

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
     * Visitor to determine if a statement is a no-op statement.
     */
    struct is_no_op_statement_vis : public boost::static_visitor<bool> {
      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return false
       */
      bool operator()(const nil& st) const;

      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return false
       */
      bool operator()(const assgn& st) const;

      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return false
       */
      bool operator()(const sample& st) const;

      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return false
       */
      bool operator()(const increment_log_prob_statement& st) const;

      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return false
       */
      bool operator()(const expression& st) const;

      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return false
       */
      bool operator()(const statements& st) const;

      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return false
       */
      bool operator()(const for_statement& st) const;

      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return false
       */
      bool operator()(const for_array_statement& st) const;

      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return false
       */
      bool operator()(const for_matrix_statement& st) const;

      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return false
       */
      bool operator()(const conditional_statement& st) const;

      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return false
       */
      bool operator()(const while_statement& st) const;

      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return false
       */
      bool operator()(const break_continue_statement& st) const;

      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return false
       */
      bool operator()(const print_statement& st) const;

      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return false
       */
      bool operator()(const reject_statement& st) const;

      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return true
       */
      bool operator()(const no_op_statement& st) const;

      /**
       * Return true if the specified statement is a no-op statement.
       *
       * @param st statement
       * @return false
       */
      bool operator()(const return_statement& st) const;
    };

  }
}
#endif
