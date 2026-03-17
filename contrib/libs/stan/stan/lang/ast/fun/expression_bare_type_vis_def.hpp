#ifndef STAN_LANG_AST_FUN_EXPRESSION_BARE_TYPE_VIS_DEF_HPP
#define STAN_LANG_AST_FUN_EXPRESSION_BARE_TYPE_VIS_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {
expression_bare_type_vis::expression_bare_type_vis() {}

bare_expr_type expression_bare_type_vis::operator()(const nil& st) const {
  return ill_formed_type();
}

bare_expr_type expression_bare_type_vis::operator()(
    const int_literal& e) const {
  return int_type();
}

bare_expr_type expression_bare_type_vis::operator()(
    const double_literal& e) const {
  return double_type();
}

bare_expr_type expression_bare_type_vis::operator()(const array_expr& e) const {
  return e.type_;
}

bare_expr_type expression_bare_type_vis::operator()(
    const matrix_expr& e) const {
  return matrix_type();
}

bare_expr_type expression_bare_type_vis::operator()(
    const row_vector_expr& e) const {
  return row_vector_type();
}

bare_expr_type expression_bare_type_vis::operator()(const variable& e) const {
  return e.type_;
}

bare_expr_type expression_bare_type_vis::operator()(const fun& e) const {
  return e.type_;
}

bare_expr_type expression_bare_type_vis::operator()(
    const integrate_1d& e) const {
  return double_type();
}

bare_expr_type expression_bare_type_vis::operator()(
    const integrate_ode& e) const {
  return bare_array_type(double_type(), 2);
}

bare_expr_type expression_bare_type_vis::operator()(
    const integrate_ode_control& e) const {
  return bare_array_type(double_type(), 2);
}

bare_expr_type expression_bare_type_vis::operator()(
    const algebra_solver& e) const {
  return vector_type();
}

bare_expr_type expression_bare_type_vis::operator()(
    const algebra_solver_control& e) const {
  return vector_type();
}

bare_expr_type expression_bare_type_vis::operator()(const map_rect& e) const {
  return vector_type();
}

bare_expr_type expression_bare_type_vis::operator()(const index_op& e) const {
  return e.type_;
}

bare_expr_type expression_bare_type_vis::operator()(
    const index_op_sliced& e) const {
  return e.type_;
}

bare_expr_type expression_bare_type_vis::operator()(
    const conditional_op& e) const {
  return e.type_;
}

bare_expr_type expression_bare_type_vis::operator()(const binary_op& e) const {
  return e.type_;
}

bare_expr_type expression_bare_type_vis::operator()(const unary_op& e) const {
  return e.type_;
}

}  // namespace lang
}  // namespace stan
#endif
