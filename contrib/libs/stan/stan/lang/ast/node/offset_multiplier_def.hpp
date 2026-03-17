#ifndef STAN_LANG_AST_NODE_OFFSET_MULTIPLIER_DEF_HPP
#define STAN_LANG_AST_NODE_OFFSET_MULTIPLIER_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {

offset_multiplier::offset_multiplier() {}

offset_multiplier::offset_multiplier(const expression &offset,
                                     const expression &multiplier)
    : offset_(offset), multiplier_(multiplier) {}

bool offset_multiplier::has_offset() const { return !is_nil(offset_.expr_); }

bool offset_multiplier::has_multiplier() const {
  return !is_nil(multiplier_.expr_);
}

}  // namespace lang
}  // namespace stan
#endif
