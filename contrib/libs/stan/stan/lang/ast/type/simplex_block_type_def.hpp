#ifndef STAN_LANG_AST_SIMPLEX_BLOCK_TYPE_DEF_HPP
#define STAN_LANG_AST_SIMPLEX_BLOCK_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {
simplex_block_type::simplex_block_type(const expression& K) : K_(K) {}

simplex_block_type::simplex_block_type() : simplex_block_type(nil()) {}

expression simplex_block_type::K() const { return K_; }
}  // namespace lang
}  // namespace stan
#endif
