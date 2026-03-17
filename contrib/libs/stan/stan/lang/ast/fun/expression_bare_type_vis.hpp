#ifndef STAN_LANG_AST_FUN_EXPRESSION_BARE_TYPE_VIS_HPP
#define STAN_LANG_AST_FUN_EXPRESSION_BARE_TYPE_VIS_HPP

#include <stan/lang/ast/nil.hpp>
#include <stan/lang/ast/node/int_literal.hpp>
#include <stan/lang/ast/node/double_literal.hpp>
#include <stan/lang/ast/node/array_expr.hpp>
#include <stan/lang/ast/node/matrix_expr.hpp>
#include <stan/lang/ast/node/row_vector_expr.hpp>
#include <stan/lang/ast/node/variable.hpp>
#include <stan/lang/ast/node/fun.hpp>
#include <stan/lang/ast/node/integrate_1d.hpp>
#include <stan/lang/ast/node/integrate_ode.hpp>
#include <stan/lang/ast/node/integrate_ode_control.hpp>
#include <stan/lang/ast/node/algebra_solver.hpp>
#include <stan/lang/ast/node/algebra_solver_control.hpp>
#include <stan/lang/ast/node/map_rect.hpp>
#include <stan/lang/ast/node/index_op.hpp>
#include <stan/lang/ast/node/index_op_sliced.hpp>
#include <stan/lang/ast/node/conditional_op.hpp>
#include <stan/lang/ast/node/binary_op.hpp>
#include <stan/lang/ast/node/unary_op.hpp>
#include <boost/variant/static_visitor.hpp>
#include <string>

namespace stan {
namespace lang {

struct expression_bare_type_vis : public boost::static_visitor<bare_expr_type> {
  /**
   * Construct a visitor.
   */
  expression_bare_type_vis();

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const nil& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const int_literal& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const double_literal& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const array_expr& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const matrix_expr& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const row_vector_expr& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const variable& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const fun& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const integrate_1d& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const integrate_ode& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const integrate_ode_control& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const algebra_solver& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const algebra_solver_control& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const map_rect& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const index_op& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const index_op_sliced& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const conditional_op& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const binary_op& e) const;

  /**
   * Return the bare_expr_type corresponding to this expression
   *
   * @return bare expression type
   */
  bare_expr_type operator()(const unary_op& e) const;
};

}  // namespace lang
}  // namespace stan
#endif
