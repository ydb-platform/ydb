#ifndef STAN_LANG_AST_FUN_BLOCK_TYPE_PARAMS_TOTAL_VIS_DEF_HPP
#define STAN_LANG_AST_FUN_BLOCK_TYPE_PARAMS_TOTAL_VIS_DEF_HPP

#include <stan/lang/ast.hpp>
#include <vector>

namespace stan {
namespace lang {
block_type_params_total_vis::block_type_params_total_vis() {}

expression block_type_params_total_vis::operator()(
    const block_array_type& x) const {
  expression result = x.contains().params_total();
  std::vector<expression> lens = x.array_lens();
  for (size_t i = 0; i < lens.size(); ++i) {
    result = binary_op(result, "*", lens[i]);
  }
  return result;
}

expression block_type_params_total_vis::operator()(
    const cholesky_factor_corr_block_type& x) const {
  // (K * (K - 1)) / 2
  int_literal one(1);
  int_literal two(2);
  return binary_op(binary_op(x.K_, "*", binary_op(x.K_, "-", one)), "/", two);
}

expression block_type_params_total_vis::operator()(
    const cholesky_factor_cov_block_type& x) const {
  // (N * (N + 1)) / 2 + (M - N) * N
  int_literal one(1);
  int_literal two(2);
  return binary_op(
      binary_op(binary_op(x.N_, "*", binary_op(x.N_, "+", one)), "/", two), "+",
      binary_op(binary_op(x.M_, "-", x.N_), "*", x.N_));
}

expression block_type_params_total_vis::operator()(
    const corr_matrix_block_type& x) const {
  // (K * (K - 1)) / 2
  int_literal one(1);
  int_literal two(2);

  return binary_op(binary_op(x.K_, "*", binary_op(x.K_, "-", one)), "/", two);
}

expression block_type_params_total_vis::operator()(
    const cov_matrix_block_type& x) const {
  // K + (K * (K - 1 ) / 2)
  int_literal one(1);
  int_literal two(2);
  return binary_op(
      x.K_, "+",
      binary_op(binary_op(x.K_, "*", binary_op(x.K_, "-", one)), "/", two));
}

expression block_type_params_total_vis::operator()(
    const double_block_type& x) const {
  return int_literal(1);
}

expression block_type_params_total_vis::operator()(
    const ill_formed_type& x) const {
  return int_literal(0);
}

expression block_type_params_total_vis::operator()(
    const int_block_type& x) const {
  return int_literal(1);
}

expression block_type_params_total_vis::operator()(
    const matrix_block_type& x) const {
  return binary_op(x.M_, "*", x.N_);
}

expression block_type_params_total_vis::operator()(
    const ordered_block_type& x) const {
  return x.K_;
}

expression block_type_params_total_vis::operator()(
    const positive_ordered_block_type& x) const {
  return x.K_;
}

expression block_type_params_total_vis::operator()(
    const row_vector_block_type& x) const {
  return x.N_;
}

expression block_type_params_total_vis::operator()(
    const simplex_block_type& x) const {
  int_literal one(1);
  return binary_op(x.K_, "-", one);
}

expression block_type_params_total_vis::operator()(
    const unit_vector_block_type& x) const {
  return x.K_;
}

expression block_type_params_total_vis::operator()(
    const vector_block_type& x) const {
  return x.N_;
}
}  // namespace lang
}  // namespace stan
#endif
