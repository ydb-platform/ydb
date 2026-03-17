#ifndef STAN_LANG_AST_COV_MATRIX_BLOCK_TYPE_DEF_HPP
#define STAN_LANG_AST_COV_MATRIX_BLOCK_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {
cov_matrix_block_type::cov_matrix_block_type(const expression& K) : K_(K) {}

cov_matrix_block_type::cov_matrix_block_type() : cov_matrix_block_type(nil()) {}

expression cov_matrix_block_type::K() const { return K_; }
}  // namespace lang
}  // namespace stan
#endif
