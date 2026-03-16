#ifndef STAN_LANG_AST_CHOLESKY_FACTOR_CORR_BLOCK_TYPE_DEF_HPP
#define STAN_LANG_AST_CHOLESKY_FACTOR_CORR_BLOCK_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {
cholesky_factor_corr_block_type::cholesky_factor_corr_block_type(
    const expression& K)
    : K_(K) {}

cholesky_factor_corr_block_type::cholesky_factor_corr_block_type()
    : cholesky_factor_corr_block_type(nil()) {}

expression cholesky_factor_corr_block_type::K() const { return K_; }
}  // namespace lang
}  // namespace stan
#endif
