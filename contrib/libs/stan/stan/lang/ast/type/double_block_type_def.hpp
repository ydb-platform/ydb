#ifndef STAN_LANG_AST_DOUBLE_BLOCK_TYPE_DEF_HPP
#define STAN_LANG_AST_DOUBLE_BLOCK_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {
double_block_type::double_block_type(const range &bounds,
                                     const offset_multiplier &ls)
    : bounds_(bounds), ls_(ls) {
  if (bounds.has_low() || bounds.has_high())
    if (ls.has_offset() || ls.has_multiplier())
      throw std::invalid_argument("Block type cannot have both a bound and"
                                  "an offset/multiplier.");
}

double_block_type::double_block_type(const range &bounds) : bounds_(bounds) {}

double_block_type::double_block_type(const offset_multiplier &ls) : ls_(ls) {}

double_block_type::double_block_type()
    : double_block_type(range(), offset_multiplier()) {}

range double_block_type::bounds() const { return bounds_; }

offset_multiplier double_block_type::ls() const { return ls_; }
}  // namespace lang
}  // namespace stan
#endif
