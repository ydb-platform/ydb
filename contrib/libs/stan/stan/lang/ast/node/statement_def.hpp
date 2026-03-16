#ifndef STAN_LANG_AST_NODE_STATEMENT_DEF_HPP
#define STAN_LANG_AST_NODE_STATEMENT_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    statement::statement() : statement_(nil()) { }

    statement::statement(const statement_t& st) : statement_(st) { }

    statement::statement(const nil& st) : statement_(st) { }

    statement::statement(const assgn& st) : statement_(st) { }

    statement::statement(const sample& st) : statement_(st) { }

    statement::statement(const increment_log_prob_statement& st)
      : statement_(st) {
    }

    statement::statement(const statements& st) : statement_(st) { }

    statement::statement(const expression& st) : statement_(st) { }

    statement::statement(const for_statement& st) : statement_(st) { }

    statement::statement(const for_array_statement& st) : statement_(st) { }

    statement::statement(const for_matrix_statement& st) : statement_(st) { }

    statement::statement(const while_statement& st) : statement_(st) { }

    statement::statement(const break_continue_statement& st)
      : statement_(st) { }

    statement::statement(const conditional_statement& st) : statement_(st) { }

    statement::statement(const print_statement& st) : statement_(st) { }

    statement::statement(const reject_statement& st) : statement_(st) { }

    statement::statement(const return_statement& st) : statement_(st) { }

    statement::statement(const no_op_statement& st) : statement_(st) { }

    bool statement::is_no_op_statement() const {
      is_no_op_statement_vis vis;
      return boost::apply_visitor(vis, statement_);
    }

  }
}
#endif
