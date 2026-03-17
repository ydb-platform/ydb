#ifndef STAN_LANG_AST_FUN_VAR_TYPE_NAME_VIS_DEF_HPP
#define STAN_LANG_AST_FUN_VAR_TYPE_NAME_VIS_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
namespace lang {
var_type_name_vis::var_type_name_vis() {}

std::string var_type_name_vis::operator()(const block_array_type& x) const {
  return "array";
}

std::string var_type_name_vis::operator()(const local_array_type& x) const {
  return "array";
}

std::string var_type_name_vis::operator()(
    const cholesky_factor_corr_block_type& x) const {
  return "cholesky_factor_corr";
}

std::string var_type_name_vis::operator()(
    const cholesky_factor_cov_block_type& x) const {
  return "cholesky_factor_cov";
}

std::string var_type_name_vis::operator()(
    const corr_matrix_block_type& x) const {
  return "corr_matrix";
}

std::string var_type_name_vis::operator()(
    const cov_matrix_block_type& x) const {
  return "cov_matrix";
}

std::string var_type_name_vis::operator()(const double_block_type& x) const {
  return "real";
}

std::string var_type_name_vis::operator()(const double_type& x) const {
  return "real";
}

std::string var_type_name_vis::operator()(const ill_formed_type& x) const {
  return "ill_formed";
}

std::string var_type_name_vis::operator()(const int_block_type& x) const {
  return "int";
}

std::string var_type_name_vis::operator()(const int_type& x) const {
  return "int";
}

std::string var_type_name_vis::operator()(const matrix_block_type& x) const {
  return "matrix";
}

std::string var_type_name_vis::operator()(const matrix_local_type& x) const {
  return "matrix";
}

std::string var_type_name_vis::operator()(const ordered_block_type& x) const {
  return "ordered";
}

std::string var_type_name_vis::operator()(
    const positive_ordered_block_type& x) const {
  return "positive_ordered";
}

std::string var_type_name_vis::operator()(
    const row_vector_block_type& x) const {
  return "row_vector";
}

std::string var_type_name_vis::operator()(
    const row_vector_local_type& x) const {
  return "row_vector";
}

std::string var_type_name_vis::operator()(const simplex_block_type& x) const {
  return "simplex";
}

std::string var_type_name_vis::operator()(
    const unit_vector_block_type& x) const {
  return "unit_vector";
}

std::string var_type_name_vis::operator()(const vector_block_type& x) const {
  return "vector";
}

std::string var_type_name_vis::operator()(const vector_local_type& x) const {
  return "vector";
}
}  // namespace lang
}  // namespace stan
#endif
