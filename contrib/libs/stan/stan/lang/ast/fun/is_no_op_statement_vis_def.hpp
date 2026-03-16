#ifndef STAN_LANG_AST_FUN_IS_NO_OP_STATEMENT_VIS_DEF_HPP
#define STAN_LANG_AST_FUN_IS_NO_OP_STATEMENT_VIS_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    bool is_no_op_statement_vis::operator()(const nil& st) const {
      return false;
    }

    bool is_no_op_statement_vis::operator()(const assgn& st) const {
      return false;
    }

    bool is_no_op_statement_vis::operator()(const sample& st) const {
      return false;
    }

    bool is_no_op_statement_vis::operator()(
                            const increment_log_prob_statement& t) const {
      return false;
    }

    bool is_no_op_statement_vis::operator()(const expression& st) const {
      return false;
    }

    bool is_no_op_statement_vis::operator()(const statements& st) const {
      return false;
    }

    bool is_no_op_statement_vis::operator()(const for_statement& st) const {
      return false;
    }

    bool is_no_op_statement_vis
      ::operator()(const for_array_statement& st) const {
      return false;
    }

    bool is_no_op_statement_vis
      ::operator()(const for_matrix_statement& st) const {
      return false;
    }

    bool is_no_op_statement_vis::operator()(
                                    const conditional_statement& st) const {
      return false;
    }

    bool is_no_op_statement_vis::operator()(const while_statement& st) const {
      return false;
    }

    bool
    is_no_op_statement_vis::operator()(const break_continue_statement& st)
      const {
      return false;
    }

    bool is_no_op_statement_vis::operator()(const print_statement& st) const {
      return false;
    }

    bool is_no_op_statement_vis::operator()(const reject_statement& st) const {
      return false;
    }

    bool is_no_op_statement_vis::operator()(const no_op_statement& st) const {
      return true;
    }

    bool is_no_op_statement_vis::operator()(const return_statement& st) const {
      return false;
    }

  }
}
#endif
