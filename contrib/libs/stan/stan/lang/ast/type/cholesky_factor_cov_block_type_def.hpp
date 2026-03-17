#ifndef STAN_LANG_AST_CHOLESKY_FACTOR_COV_BLOCK_TYPE_DEF_HPP
#define STAN_LANG_AST_CHOLESKY_FACTOR_COV_BLOCK_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {
cholesky_factor_cov_block_type::cholesky_factor_cov_block_type(
    const expression& M, const expression& N)
    : M_(M), N_(N) {}

cholesky_factor_cov_block_type::cholesky_factor_cov_block_type()
    : cholesky_factor_cov_block_type(nil(), nil()) {}

expression cholesky_factor_cov_block_type::M() const { return M_; }

expression cholesky_factor_cov_block_type::N() const { return N_; }
}  // namespace lang
}  // namespace stan
#endif
