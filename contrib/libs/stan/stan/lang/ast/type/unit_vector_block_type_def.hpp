#ifndef STAN_LANG_AST_UNIT_VECTOR_BLOCK_TYPE_DEF_HPP
#define STAN_LANG_AST_UNIT_VECTOR_BLOCK_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {
unit_vector_block_type::unit_vector_block_type(const expression& K) : K_(K) {}

unit_vector_block_type::unit_vector_block_type()
    : unit_vector_block_type(nil()) {}

expression unit_vector_block_type::K() const { return K_; }
}  // namespace lang
}  // namespace stan
#endif
