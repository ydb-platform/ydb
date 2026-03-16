#ifndef STAN_LANG_AST_VECTOR_BLOCK_TYPE_DEF_HPP
#define STAN_LANG_AST_VECTOR_BLOCK_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {
vector_block_type::vector_block_type(const range &bounds,
                                     const offset_multiplier &ls,
                                     const expression &N)
    : bounds_(bounds), ls_(ls), N_(N) {
  if (bounds.has_low() || bounds.has_high())
    if (ls.has_offset() || ls.has_multiplier())
      throw std::invalid_argument("Block type cannot have both a bound and"
                                  "a offset/multiplier.");
}

vector_block_type::vector_block_type(const range &bounds, const expression &N)
    : bounds_(bounds), ls_(offset_multiplier()), N_(N) {}

vector_block_type::vector_block_type(const offset_multiplier &ls,
                                     const expression &N)
    : bounds_(range()), ls_(ls), N_(N) {}

vector_block_type::vector_block_type() : vector_block_type(range(), nil()) {}

range vector_block_type::bounds() const { return bounds_; }

offset_multiplier vector_block_type::ls() const { return ls_; }

expression vector_block_type::N() const { return N_; }
}  // namespace lang
}  // namespace stan
#endif
