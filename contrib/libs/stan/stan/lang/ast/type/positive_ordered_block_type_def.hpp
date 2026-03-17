#ifndef STAN_LANG_AST_POSITIVE_ORDERED_BLOCK_TYPE_DEF_HPP
#define STAN_LANG_AST_POSITIVE_ORDERED_BLOCK_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {
positive_ordered_block_type::positive_ordered_block_type(const expression& K)
    : K_(K) {}

positive_ordered_block_type::positive_ordered_block_type()
    : positive_ordered_block_type(nil()) {}

expression positive_ordered_block_type::K() const { return K_; }
}  // namespace lang
}  // namespace stan
#endif
