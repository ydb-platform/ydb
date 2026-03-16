#ifndef STAN_LANG_AST_MATRIX_BLOCK_TYPE_DEF_HPP
#define STAN_LANG_AST_MATRIX_BLOCK_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {
matrix_block_type::matrix_block_type(const range &bounds,
                                     const offset_multiplier &ls,
                                     const expression &M, const expression &N)
    : bounds_(bounds), ls_(ls), M_(M), N_(N) {
  if (bounds.has_low() || bounds.has_high())
    if (ls.has_offset() || ls.has_multiplier())
      throw std::invalid_argument("Block type cannot have both a bound and"
                                  "a offset/multiplier.");
}

matrix_block_type::matrix_block_type(const range &bounds, const expression &M,
                                     const expression &N)
    : bounds_(bounds), ls_(offset_multiplier()), M_(M), N_(N) {}

matrix_block_type::matrix_block_type(const offset_multiplier &ls,
                                     const expression &M, const expression &N)
    : bounds_(range()), ls_(ls), M_(M), N_(N) {}

matrix_block_type::matrix_block_type()
    : matrix_block_type(range(), nil(), nil()) {}

range matrix_block_type::bounds() const { return bounds_; }

offset_multiplier matrix_block_type::ls() const { return ls_; }

expression matrix_block_type::M() const { return M_; }

expression matrix_block_type::N() const { return N_; }
}  // namespace lang
}  // namespace stan
#endif
