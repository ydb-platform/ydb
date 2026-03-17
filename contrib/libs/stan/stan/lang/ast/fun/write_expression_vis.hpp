#ifndef STAN_LANG_AST_FUN_WRITE_EXPRESSION_VIS_HPP
#define STAN_LANG_AST_FUN_WRITE_EXPRESSION_VIS_HPP

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
#include <ostream>
#include <string>

namespace stan {
namespace lang {

/**
 * Visitor to format expression for parser error messages.
 */
struct write_expression_vis : public boost::static_visitor<std::string> {
  /**
   * Construct a visitor.
   */
  write_expression_vis();

  /**
   * Return string representation for expression.
   */
  std::string operator()(const nil& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const int_literal& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const double_literal& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const array_expr& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const matrix_expr& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const row_vector_expr& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const variable& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const fun& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const integrate_1d& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const integrate_ode& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const integrate_ode_control& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const algebra_solver& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const algebra_solver_control& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const map_rect& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const index_op& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const index_op_sliced& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const conditional_op& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const binary_op& e) const;

  /**
   * Return string representation for expression.
   */
  std::string operator()(const unary_op& e) const;
};

}  // namespace lang
}  // namespace stan
#endif
