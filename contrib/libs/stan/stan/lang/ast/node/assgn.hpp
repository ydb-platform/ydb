#ifndef STAN_LANG_AST_NODE_ASSIGN_HPP
#define STAN_LANG_AST_NODE_ASSIGN_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <stan/lang/ast/node/idx.hpp>
#include <stan/lang/ast/node/variable.hpp>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * AST node for assignment to variable with multi-indexing.
     */
    struct assgn {
      /**
       * Construct an uninitialized assignment statement.
       */
      assgn();

      /**
       * Construct an assignment statement with the specified
       * variable, indexing, and right-hand side expression.
       *
       * @param[in] lhs_var variable being assigned
       * @param[in] idxs sequence of indexes indicating element being
       * assigned 
       * @param[in] op assignment operator
       * @param[in] rhs value of assignment
       */
      assgn(const variable& lhs_var, const std::vector<idx>& idxs,
            const std::string& op, const expression& rhs);

      /**
       * Return true if the statement is assignment only
       * i.e., operator isn't a compound operator-assigment
       *
       * @return true if the statement is assignment only
       */
      bool is_simple_assignment() const;

      /**
       * Return true if any of the indexes on the lhs element
       * are sliced indexes
       *
       * @return true if lhs has sliced idx
       */
      bool lhs_var_has_sliced_idx() const;

      /**
       * Return true if the variable being assigned is a subexpression
       * of the value expression.
       *
       * @return true if the assigned variable appears in the value
       * expression 
       */
      bool lhs_var_occurs_on_rhs() const;

      /**
       * The variable being assigned.
       */
      variable lhs_var_;

      /**
       * Position(s) in variable being assigned.
       */
      std::vector<idx> idxs_;

      /**
       * Assignment operator string
       */
      std::string op_;

      /**
       * Stan math function name, (see `src/stan/lang/function_signatures.h`).
       * Left unset for simple assignment or when both operands are primitive.
       */
      std::string op_name_;

      /**
       * Value being assigned to left hand side variable at indexing
       * position. 
       */
      expression rhs_;
    };
  }
}
#endif

