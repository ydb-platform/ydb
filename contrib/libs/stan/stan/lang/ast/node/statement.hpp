#ifndef STAN_LANG_AST_NODE_STATEMENT_HPP
#define STAN_LANG_AST_NODE_STATEMENT_HPP

#include <boost/variant/recursive_variant.hpp>
#include <cstddef>

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
    struct return_statement;
    struct no_op_statement;

    /**
     * Structure to wrap the variant type of statements.
     */
    struct statement {
      /**
       * The variant type of statements.
       */
      typedef boost::variant<boost::recursive_wrapper<nil>,
                     boost::recursive_wrapper<assgn>,
                     boost::recursive_wrapper<sample>,
                     boost::recursive_wrapper<increment_log_prob_statement>,
                     boost::recursive_wrapper<expression>,
                     boost::recursive_wrapper<statements>,
                     boost::recursive_wrapper<for_statement>,
                     boost::recursive_wrapper<for_array_statement>,
                     boost::recursive_wrapper<for_matrix_statement>,
                     boost::recursive_wrapper<conditional_statement>,
                     boost::recursive_wrapper<while_statement>,
                     boost::recursive_wrapper<break_continue_statement>,
                     boost::recursive_wrapper<print_statement>,
                     boost::recursive_wrapper<reject_statement>,
                     boost::recursive_wrapper<return_statement>,
                     boost::recursive_wrapper<no_op_statement> >
      statement_t;

      /**
       * Construct a default (nil) statement.
       */
      statement();

      /**
       * Construct a statement from the specified raw variant type
       * wrapper.
       *
       * @param st statement variant type
       */
      statement(const statement_t& st);  // NOLINT(runtime/explicit)

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const nil& st);  // NOLINT(runtime/explicit)

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const assgn& st);  // NOLINT(runtime/explicit)

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const sample& st);  // NOLINT(runtime/explicit)

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const increment_log_prob_statement& st);  // NOLINT

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const expression& st);  // NOLINT(runtime/explicit)

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const statements& st);  // NOLINT(runtime/explicit)

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const for_statement& st);  // NOLINT(runtime/explicit)

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const for_array_statement& st);  // NOLINT(runtime/explicit)

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const for_matrix_statement& st);  // NOLINT(runtime/explicit)

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const conditional_statement& st);  // NOLINT(runtime/explicit)

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const while_statement& st);  // NOLINT(runtime/explicit)

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const break_continue_statement& st);  // NOLINT

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const print_statement& st);  // NOLINT(runtime/explicit)

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const reject_statement& st);  // NOLINT(runtime/explicit)

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const no_op_statement& st);  // NOLINT(runtime/explicit)

      /**
       * Construct a statement variant type wrapper from the specified
       * basic statement.
       *
       * @param st basic statement
       */
      statement(const return_statement& st);  // NOLINT(runtime/explicit)

      /**
       * Return true if the basic statement held by the variant type
       * in this wrapper is the no-op statement.
       *
       * @return true if this is a no-op statement wrapper
       */
      bool is_no_op_statement() const;

      /**
       * The statement variant type held by this wrapper.
       */
      statement_t statement_;

      /**
       * The line in the source code where the statement begins.
       */
      std::size_t begin_line_;

      /**
       * The line in the source code where the statement ends.
       */
      std::size_t end_line_;
    };

  }
}
#endif
