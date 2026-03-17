#ifndef STAN_LANG_AST_CORR_MATRIX_BLOCK_TYPE_DEF_HPP
#define STAN_LANG_AST_CORR_MATRIX_BLOCK_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {
corr_matrix_block_type::corr_matrix_block_type(const expression& K) : K_(K) {}

corr_matrix_block_type::corr_matrix_block_type()
    : corr_matrix_block_type(nil()) {}

expression corr_matrix_block_type::K() const { return K_; }
}  // namespace lang
}  // namespace stan
#endif
