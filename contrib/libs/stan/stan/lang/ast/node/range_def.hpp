#ifndef STAN_LANG_AST_NODE_RANGE_DEF_HPP
#define STAN_LANG_AST_NODE_RANGE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    range::range() { }

    range::range(const expression& low, const expression& high)
      : low_(low), high_(high) {  }

    bool range::has_low() const {
      return !is_nil(low_.expr_);
    }

    bool range::has_high() const {
      return !is_nil(high_.expr_);
    }

  }
}
#endif
