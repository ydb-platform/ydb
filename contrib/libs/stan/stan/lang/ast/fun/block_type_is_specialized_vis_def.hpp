#ifndef STAN_LANG_AST_FUN_BLOCK_TYPE_IS_SPECIALIZED_VIS_DEF_HPP
#define STAN_LANG_AST_FUN_BLOCK_TYPE_IS_SPECIALIZED_VIS_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {
block_type_is_specialized_vis::block_type_is_specialized_vis() {}

bool block_type_is_specialized_vis::operator()(
    const block_array_type& x) const {
  return x.contains().is_specialized();
}

bool block_type_is_specialized_vis::operator()(
    const cholesky_factor_corr_block_type& x) const {
  return true;
}

bool block_type_is_specialized_vis::operator()(
    const cholesky_factor_cov_block_type& x) const {
  return true;
}

bool block_type_is_specialized_vis::operator()(
    const corr_matrix_block_type& x) const {
  return true;
}

bool block_type_is_specialized_vis::operator()(
    const cov_matrix_block_type& x) const {
  return true;
}

bool block_type_is_specialized_vis::operator()(
    const double_block_type& x) const {
  return false;
}

bool block_type_is_specialized_vis::operator()(const ill_formed_type& x) const {
  return false;
}

bool block_type_is_specialized_vis::operator()(const int_block_type& x) const {
  return false;
}

bool block_type_is_specialized_vis::operator()(
    const matrix_block_type& x) const {
  return false;
}

bool block_type_is_specialized_vis::operator()(
    const ordered_block_type& x) const {
  return true;
}

bool block_type_is_specialized_vis::operator()(
    const positive_ordered_block_type& x) const {
  return true;
}

bool block_type_is_specialized_vis::operator()(
    const row_vector_block_type& x) const {
  return false;
}

bool block_type_is_specialized_vis::operator()(
    const simplex_block_type& x) const {
  return true;
}

bool block_type_is_specialized_vis::operator()(
    const unit_vector_block_type& x) const {
  return true;
}

bool block_type_is_specialized_vis::operator()(
    const vector_block_type& x) const {
  return false;
}
}  // namespace lang
}  // namespace stan
#endif
