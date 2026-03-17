#ifndef STAN_LANG_AST_INT_BLOCK_TYPE_DEF_HPP
#define STAN_LANG_AST_INT_BLOCK_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    int_block_type::int_block_type()
      : bounds_(nil(), nil()) { }

    int_block_type::int_block_type(const range& bounds)
      : bounds_(bounds) { }

    range int_block_type::bounds() const { return bounds_; }
  }
}
#endif
